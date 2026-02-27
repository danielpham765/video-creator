## Architecture overview (Cursor context)

### Purpose

- **Project**: `video-downloader-api`
- **Goal**: NestJS-based API plus worker that downloads Bilibili videos in the background and writes merged `.mp4` files to a `data/` directory.
- **Pattern**: HTTP controller → Bull queue (`downloads`) → worker processor (`WorkerProcessor`) → local filesystem (+ job-history JSON).

### Main modules and files

- **Download API**
  - `src/download/download.module.ts`
    - Registers Bull queue: `downloads`.
    - Imports: `JobsModule`, `PlayurlModule`, `FfmpegModule`.
    - Exposes: `DownloadController`, `DownloadService`.
  - `src/download/download.controller.ts`
    - `POST /download`
      - Body: `{ bvid: string, cookies?: string, ... }` (not strongly typed).
      - Calls `DownloadService.enqueue()`; returns `{ jobId }`.
    - `GET /download/status/:id`
      - Looks up Bull job and merges:
        - `state`, `progress`, `failedReason`, `returnvalue`.
        - History from `JobHistoryService.getHistory(id)`.
    - `POST /download/:id/resume`
      - Emits a stop signal for job `<id>`:
        - Prefer Redis key `job:stop:<id>` via the underlying Bull client.
        - Fallback: `data/<id>.stop` marker file.
      - Waits up to ~15s for worker to append a `stopped` event.
      - Clears stop marker and re-enqueues job with same payload (attempts: 3, backoff: 5000).
    - `POST /download/:id/cancel`
      - Writes `data/<id>.cancel` marker file.
      - Attempts `job.remove()` and appends `cancelled` history.
  - `src/download/download.service.ts`
    - Injects `Queue` (`downloads`).
    - `enqueue(payload)` → `downloads.add(payload, { attempts: 3, backoff: 5000 })`.

- **Playurl (Bilibili API)**
  - `src/playurl/playurl.module.ts`
    - Exports `PlayurlService`.
  - `src/playurl/playurl.service.ts`
    - `getCidFromBvid(bvid)`
      - GET `https://api.bilibili.com/x/player/pagelist?bvid=<bvid>`.
      - Returns `data[0].cid`.
    - `getPlayurl(bvid, cid, cookies?)`
      - GET `https://api.bilibili.com/x/player/playurl?...`.
      - Adds `Referer` and optional `Cookie` headers.
      - Returns raw response JSON (includes `data.dash` and/or `data.durl`).

- **FFmpeg merge**
  - `src/ffmpeg/ffmpeg.module.ts`
    - Exports `FfmpegService`.
  - `src/ffmpeg/ffmpeg.service.ts`
    - Uses `fluent-ffmpeg`.
    - `merge(videoUrl, audioUrl, outputPath, headers?)`
      - Inputs are URLs (or file paths).
      - Uses `-c copy` to merge without re-encoding.

- **Job history**
  - `src/jobs/jobs.module.ts`
    - Exports `JobHistoryService`.
  - `src/jobs/job-history.service.ts`
    - Base dir: `data/jobs/`.
    - `appendEvent(jobId, event)`
      - Appends `{ ...event, ts }` to `data/jobs/<jobId>.json`.
    - `getHistory(jobId)`
      - Reads and parses the JSON file or returns `[]`.

- **Worker / queue processor**
  - `src/worker/worker.processor.ts`
    - Decorated with `@Processor('downloads')`.
    - Injects: `PlayurlService`, `FfmpegService`, `JobHistoryService`, `Queue (downloads)`.
    - Base data dir: `data/`.
    - For each job:
      1. Validates `bvid` and reads `cookies` from `job.data`.
      2. Resolves `cid` and `playurl`; logs and checks `play.code === 0`.
      3. Sets up cancel/stop markers:
         - Files: `data/<jobId>.cancel`, `data/<jobId>.stop`.
         - Redis key: `job:stop:<jobId>` (stop flag).
         - Optionally creates a Redis subscriber or falls back to polling.
      4. If `play.data.dash` is present:
         - Extracts first `video.baseUrl`, `audio.baseUrl`.
         - Temp files: `data/<jobId>-video`, `data/<jobId>-audio`.
         - Output: `data/<bvid>-<jobId>.mp4`.
         - Uses `resumeDownload` twice:
           - Headers: `{ Referer: <video page>, Cookie: cookies }`.
           - Cancel: `data/<jobId>.cancel`.
           - Stop: `() => stopRequested`.
         - On success: calls `ffmpeg.merge(videoTmp, audioTmp, outFile)`.
         - Cleans temp and marker files.
         - Appends `finished` history with `result.path`.
      5. Else if `play.data.durl` is present:
         - Uses `durl[0].url` as `url`.
         - Output: `data/<bvid>-<jobId>.mp4`.
         - Uses `resumeDownload(url, outFile, headers, cancelFile)`.
         - Handles stop/cancel markers and history accordingly.
      6. Else:
         - Throws error `"No downloadable urls found or DRM-protected"`.
      7. On any error:
         - Logs via Nest `Logger`.
         - Tries to set job progress to 0 and appends `failed` history with `message`.

- **Download/resume utility**
  - `src/utils/resume-download.ts`
    - Public API: `resumeDownload(url, dest, headers?, cancelFilePath?, stopFileOrCheck?)`.
    - Behavior:
      - Resumes **idempotent** HTTP downloads using `Range` and file size.
      - Uses `axios` stream with `validateStatus` allowing `200` and `206`.
      - If server ignores `Range` on resume (returns `200`), deletes file and restarts.
      - Writes to a file stream (`a` when resuming, `w` otherwise).
      - Periodically logs progress, using content-length when available.
      - Cooperative cancellation:
        - If `cancelFilePath` exists, destroys stream with `"download cancelled"`.
        - If `stopFileOrCheck` is:
          - a string path: existence → `"download stopped"`.
          - a function: returns true → `"download stopped"`.

### Data and state model

- **Filesystem**
  - `data/`
    - `<bvid>-<jobId>.mp4` – final merged or direct-download file.
    - `<jobId>-video`, `<jobId>-audio` – DASH intermediate files.
    - `<jobId>.cancel` – cancel marker (written by controller, read by worker and `resumeDownload`).
    - `<jobId>.stop` – stop marker fallback when Redis is unavailable.
    - `jobs/`
      - `<jobId>.json` – array of history events with timestamps.

- **Redis/Bull**
  - Queue name: `downloads`.
  - Jobs:
    - Data: at minimum `{ bvid, cookies? }`.
    - Progress: updated at major lifecycle stages (resolving, downloading, merging, finished, failed).
  - Stop coordination:
    - Key `job:stop:<id>` is written by controller on resume request.
    - Worker either:
      - Subscribes to this key via Redis pub/sub, or
      - Polls via `GET job:stop:<id>` if pub/sub not available.
    - When triggered, the worker sets `stopRequested = true`, which propagates to `resumeDownload`.

### Public API surface (for reference)

- **Endpoints** (see `README.md` for user-facing description)
  - `POST /download` → `{ jobId }`.
  - `GET /download/status/:id` → `{ id, state, progress, failedReason, result, history }`.
  - `POST /download/:id/resume` → `{ status: 'resume-enqueued', newJobId }`.
  - `POST /download/:id/cancel` → `{ status: 'cancelled', id }`.

### Parallel segmented downloads

- **Design document**: `plan/PLAN_parallel_downloads.md`.
- **Queues**:
  - `downloads` – master/orchestrator jobs.
  - `download-parts` – byte-range part jobs.
- **Current behavior**:
  - For DASH (`play.data.dash`): existing single-stream video+audio download + ffmpeg merge is used.
  - For large single-URL downloads (`play.data.durl[0].url`) that support `Accept-Ranges: bytes`:
    - Master job performs a HEAD request to discover `content-length`.
    - If the file is large enough (at least ~2× 8MB), it:
      - Builds a manifest of ~8MB byte ranges.
      - Persists the manifest to `data/<jobId>/manifest.json` and to Redis under `job:parts:<jobId>`.
      - Enqueues one Bull job per part into `download-parts`.
    - `PartsProcessor` downloads each byte-range into `data/<jobId>/parts/part-<index>.bin` using `resumeDownload(onProgress)`.
    - The master polls part jobs every 5s, detects stalls after ~15s of no progress, and can mark the whole job as failed.
    - When all parts complete, the master merges the part files into a single output `.mp4`.
- **Endpoints**:
  - `GET /download/status/:id` – still the main status endpoint; future work may expose more part-level detail from Redis.
  - `POST /download/:id/merge-partial` – planned for best-effort partial merge based on completed parts (see plan doc).

### Other plans

- **Original NestJS project plan**
  - Document: `plan/PLAN_nestjs.md`.
  - Describes intended modules (API, Playurl, Download, FFmpeg, Queue, Storage), Docker setup, and endpoints design.

