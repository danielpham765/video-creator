## Video Downloader API – Developer Architecture

### High-level overview

- **Purpose**: Background-friendly Bilibili video downloader. The HTTP API accepts a `bvid` (and optional cookies), resolves playback URLs via Bilibili APIs, downloads streams reliably (with resume/cancel/stop), merges audio+video with `ffmpeg`, and writes result files to a local `data/` directory.
- **Style**: NestJS modules + Bull queue workers. The HTTP layer is thin; almost all work happens in a Bull processor that can run in a separate worker process/container.
- **Persistence**: No DB. Job state is in Bull/Redis; job history and marker files live under `data/`.

### Core runtime processes

- **API process** (`nest start` / `start:dev`)
  - Exposes REST endpoints under `/download`.
  - Enqueues background jobs into the `downloads` Bull queue.
  - Reads job status/result from Bull and from job-history JSON files.
  - Emits stop/cancel signals via Redis and filesystem markers.

- **Worker process** (`node dist/worker.js`, see `WorkerProcessor`)
  - Subscribes to the `downloads` queue.
  - For each job:
    - Resolves Bilibili `cid` and `playurl`.
    - Chooses between DASH (`dash`) vs fallback `durl` download strategy.
    - Uses `resumeDownload()` to fetch stream(s) robustly with resume support.
    - Invokes `FfmpegService.merge()` when separate audio/video streams exist.
    - Writes progress and history events.
    - Honors stop/cancel signals cooperatively.

### Module and service map

- **`DownloadModule`** (`src/download/download.module.ts`)
  - Imports:
    - `BullModule.registerQueue({ name: 'downloads' })`
    - `JobsModule` (job history)
    - `PlayurlModule` (Bilibili API client)
    - `FfmpegModule` (merge helper)
  - Declares:
    - `DownloadController`
    - `DownloadService`

- **`DownloadController`** (`src/download/download.controller.ts`)
  - **Routes**
    - `POST /download`
      - Body: loosely-typed `any`, expected to carry at least `{ bvid, cookies? }`.
      - Calls `DownloadService.enqueue(body)` which enqueues to `downloads` queue.
      - Returns `{ jobId }` for client-side polling.
    - `GET /download/status/:id`
      - Looks up a Bull job by id.
      - Returns:
        - `state` (`waiting` | `active` | `completed` | `failed` | `delayed` | `not-found`, etc.)
        - `progress` (number)
        - `failedReason` (string | null)
        - `result` (worker return value, usually `{ path }` or flags like `{ cancelled: true }`)
        - `history` (array of events from `JobHistoryService`)
    - `POST /download/:id/resume`
      - Coordinates a **cooperative stop then resume** for a running job:
        1. Resolves the Bull job and reads its `data` (expects `bvid`, `cookies`).
        2. Emits a stop signal:
           - Prefer Redis: uses the underlying Bull Redis client to `SET job:stop:<id>`.
           - Fallback: writes a `data/<id>.stop` marker file.
        3. Appends `{ state: 'stop-requested' }` to history.
        4. Polls history for up to ~15s until it sees a `stopped` event from the worker.
        5. Clears the stop key/marker.
        6. Re-enqueues a new job, trying to reuse the same job id first, with `{ attempts: 3, backoff: 5000 }`.
        7. Appends `{ state: 'resume-requested' }` and returns `{ status: 'resume-enqueued', newJobId }`.
    - `POST /download/:id/cancel`
      - Records cancellation intent:
        - Writes `data/<id>.cancel` marker file.
        - Attempts to `job.remove()`; if the job is active, worker will still see the cancel marker.
        - Appends `{ state: 'cancelled' }` to history.
      - Returns `{ status: 'cancelled', id }`.

- **`DownloadService`** (`src/download/download.service.ts`)
  - Very thin abstraction over Bull:
    - Injects `Queue` instance for `downloads`.
    - `enqueue(payload)` → `downloads.add(payload, { attempts: 3, backoff: 5000 })`.

- **`PlayurlModule` / `PlayurlService`** (`src/playurl`)
  - `getCidFromBvid(bvid)`
    - Calls `https://api.bilibili.com/x/player/pagelist?bvid=<bvid>`.
    - Returns `data[0].cid`.
  - `getPlayurl(bvid, cid, cookies?)`
    - Calls `https://api.bilibili.com/x/player/playurl?bvid=<bvid>&cid=<cid>&qn=116&fnval=16`.
    - Adds `Referer` header and optional `Cookie` header.
    - Returns raw JSON body (Bilibili response).

- **`FfmpegModule` / `FfmpegService`** (`src/ffmpeg`)
  - Wraps `fluent-ffmpeg`:
    - `merge(videoUrl, audioUrl, outputPath, headers?)`
      - Builds a `ffmpeg()` pipeline with two inputs (video, audio).
      - Uses `-c copy` for container-level merge (no re-encode).
      - Resolves on `end`, rejects on `error`.
  - Headers are currently not wired into `fluent-ffmpeg` inputs; if needed, future work might add per-input options to forward cookies/Referer.

- **`JobsModule` / `JobHistoryService`** (`src/jobs`)
  - Directory: `data/jobs/`.
  - `appendEvent(jobId, event)`
    - Reads existing `data/jobs/<jobId>.json` (if any) as an array.
    - Appends `{ ...event, ts }` (auto timestamp).
    - Writes back as pretty-printed JSON.
  - `getHistory(jobId)`
    - Reads and parses the JSON file; returns `[]` on error/missing file.
  - Used from:
    - `DownloadController` (`status`, `resume`, `cancel` endpoints).
    - `WorkerProcessor` to record lifecycle events.

- **`WorkerProcessor`** (`src/worker/worker.processor.ts`)
  - Decorated with `@Processor('downloads')`, so every job in the `downloads` queue lands here.
  - **Input payload** (convention, not enforced by DTOs):
    - `{ bvid: string, cookies?: string }`
  - **Main flow** (`handleJob(job)`):
    - Validates `bvid`.
    - Emits early history (`queued`, `resolving`).
    - Resolves `cid` then `playurl`.
    - Inspects `play.data.dash` vs `play.data.durl`:
      - **DASH path**:
        - Picks `videoUrl`, `audioUrl` from first entries.
        - Defines temp paths:
          - `data/<jobId>-video`
          - `data/<jobId>-audio`
        - Defines final output:
          - `data/<bvid>-<jobId>.mp4`
        - Before starting download:
          - If cancel marker exists: mark `cancelled` and short-circuit.
          - If stop marker or `stopRequested` flag is set: mark `stopped`, clean up Redis markers, and return.
        - Downloads:
          - `video` first (`state: 'downloading-video'`, progress ~20–50).
          - `audio` next (`state: 'downloading-audio'`, progress ~50–70).
          - Both use `resumeDownload()` with headers:
            - `Referer: https://www.bilibili.com/video/<bvid>`
            - `Cookie: <cookies>`
            - `cancelFile` path and a `stopRequested` lambda.
          - If `resumeDownload` throws with message containing `"stopped"`, worker records `stopped` and exits cooperatively.
        - Merge:
          - `state: 'merging'`, progress ~70–95.
          - `ffmpeg.merge(videoTmp, audioTmp, outFile)`.
          - Cleans up temp files and trailing stop/cancel markers.
          - `state: 'finished'`, progress `100`, result `{ path: outFile }`.
      - **Single-URL (`durl`) path**:
        - Uses first `durl[0].url` as `url`.
        - Output path: `data/<bvid>-<jobId>.mp4`.
        - Early cancel check as above.
        - `state: 'downloading'`, progress ~30–100.
        - Calls `resumeDownload(url, outFile, headers, cancelFile)`.
        - Honors stop/cancel via post-download checks; may record `stopped`/`cancelled`.
        - On success: `state: 'finished'`, `result: { path: outFile }`.
    - If neither `dash` nor `durl` exist:
      - Throws `"No downloadable urls found or DRM-protected"`.
      - Logs error and records `failed` in history.

- **`resumeDownload` util** (`src/utils/resume-download.ts`)
  - HTTP streaming downloader with **resume + cooperative stop/cancel**:
    - Checks for existing `dest` file and resumes via `Range: bytes=<size>-`.
    - Uses `axios` in `stream` mode, accepting `2xx` and `206`.
    - If server ignores `Range` (returns `200` with full body), it deletes partial file and restarts.
    - Writes to a `WriteStream` in append or write mode.
    - Logs progress (bytes and percent if content-length known).
    - On each chunk:
      - If `cancelFilePath` exists → destroys stream with `"download cancelled"`.
      - If `stopFileOrCheck` is:
        - a **string**: existence of that file triggers `"download stopped"`.
        - a **function**: when it returns true, triggers `"download stopped"`.

### Data layout on disk

- **Root** (relative to `process.cwd()`):
  - `data/`
    - `<bvid>-<jobId>.mp4` – final outputs.
    - `<jobId>-video`, `<jobId>-audio` – transient DASH download files.
    - `<jobId>.cancel` – cancel marker, written by controller; read by worker and `resumeDownload`.
    - `<jobId>.stop` – stop marker alternative to Redis pub/sub.
    - `jobs/`
      - `<jobId>.json` – append-only array of history events.

### Queue & Redis integration

- **Queue name**: `downloads`
  - Registered in `DownloadModule` via `BullModule.registerQueue({ name: 'downloads' })`.
  - Same queue is injected into:
    - `DownloadController`
    - `DownloadService`
    - `WorkerProcessor`
- **Stop signaling**:
  - Redis key: `job:stop:<id>`.
  - Worker behavior:
    - Attempts to `duplicate()` the Bull client to create a pub/sub subscriber for `stopKey`.
    - If pub/sub unavailable, falls back to polling `GET job:stop:<id>` every 250ms.
    - Maintains an internal `stopRequested` flag that is passed to `resumeDownload` as a function.
- **Cancel signaling**:
  - No Redis; purely filesystem-based `data/<id>.cancel`.
  - Checked in worker and by `resumeDownload`.

### Error handling and job states (practical view)

- **Common states in job history**:
  - `queued`, `resolving`
  - `downloading-video`, `downloading-audio`, `downloading`
  - `merging`
  - `finished` (with `result.path`)
  - `failed` (with `message`)
  - `cancelled`
  - `stopped`, `stop-requested`, `stop-timeout`, `resume-requested`, `requeueing`, `requeue-failed`

- **Typical failures**:
  - Network/axios errors while calling Bilibili APIs or downloading streams.
  - `playurl` returning non-zero `code` (auth/DRM/region issues).
  - `ffmpeg` merge failures (bad inputs, disk issues).
  - These are logged via Nest `Logger` and recorded as `failed` events.

### Extensibility notes

- **New sources**:
  - You can plug new providers into the same queue by:
    - Extending the payload shape (e.g. `type: 'bilibili' | 'youtube'`).
    - Introducing additional resolver services similar to `PlayurlService`.
    - Branching inside `WorkerProcessor.handleJob`.

- **Richer API contracts**:
  - Current controllers use `any` for request bodies and raw objects for responses.
  - For stronger typing and validation:
    - Add DTOs with `class-validator`/`class-transformer`.
    - Add Swagger decorators on controllers and enable OpenAPI with `@nestjs/swagger`.

- **Parallel segmented downloads**:
  - See `plan/PLAN_parallel_downloads.md` for a design to split downloads into multiple sub-jobs (segments or byte ranges) and merge at the end.
  - That design builds on:
    - `resumeDownload` for per-part robustness.
    - Additional Bull queues (e.g. `download-parts`) and a “master” orchestration job.

### Local development references

- **Entrypoints & configuration**
  - `nest-cli.json` – `sourceRoot: "src"`.
  - `package.json` – scripts:
    - `start:dev`, `build`, `start:prod`, `start:worker`, `lint`.
  - `README.md` – docker-compose development flow and primary endpoints.

