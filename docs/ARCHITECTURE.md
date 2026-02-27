## Video Creator API Architecture (AI Cache)

Last updated: 2026-02-27
Source of truth for Cursor, Codex, and Antigravity AI.

### 1) Purpose

- Project: `video-creator` (NestJS + Bull + Redis + FFmpeg).
- Primary goal: accept a Bilibili URL/BVID, download media in background jobs, and write final `.mp4` files under `data/`.
- Persistence model:
  - Queue/job state in Bull + Redis.
  - Durable local artifacts/history in filesystem (`data/`).
  - No relational database.

### 2) Runtime Topology

- API process:
  - Bootstrapped by [`src/main.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/main.ts).
  - Loads dynamic [`AppModule.register()`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/app.module.ts).
  - Exposes REST endpoints and Swagger (`/api`, `/docs`).
  - Enqueues jobs to `downloads`.
- Worker process:
  - Enabled when `WORKER=true`.
  - Loads [`WorkerModule`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/worker.module.ts).
  - Consumes `downloads` and `download-parts` queues.

### 3) Queue Model

- Queue `downloads`:
  - Orchestration job per requested download.
  - Processor: [`WorkerProcessor`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/worker.processor.ts).
- Queue `download-parts`:
  - Sub-jobs for byte-range or segmented part downloads.
  - Processor: [`PartsProcessor`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/parts.processor.ts).

### 4) Request and Processing Flow

1. Client calls `POST /download` with DTO `CreateDownloadDto` (`url` required, `title` optional).
2. [`DownloadController.startDownload()`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/download/download.controller.ts) extracts `bvid` from URL (or accepts `payload.bvid` if present), then enqueues a `downloads` job.
3. `WorkerProcessor` resolves `cid` and play URL via [`PlayurlService`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/playurl/playurl.service.ts).
4. Worker chooses one strategy:
   - `dash-segmented`: detect HLS/MPD segments and enqueue `download-parts` jobs.
   - `dash-single`: download whole video/audio streams.
   - `durl-byte-range`: split large single URL into byte-range parts and enqueue `download-parts`.
   - direct fallback if splitting is not viable.
5. Parts are merged (concat if needed), then audio+video are merged by [`FfmpegService`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/ffmpeg/ffmpeg.service.ts).
6. Final output is moved to `data/bilibili/<safeTitle>/<bvid>-<jobId>.mp4`.
7. History events are appended to `data/jobs/<jobId>.json`.

### 5) Public API (Current)

- `POST /download`
  - Body: `{ url: string, title?: string }` (validated via DTO).
  - Response: `{ jobId }`.
- `GET /download/status/:id`
  - Returns Bull job state/progress/failure/result + job history.
  - Includes parts summary from Redis key `job:parts:<id>` when available.
- `POST /download/:id/resume`
  - Sends stop signal for active job and re-enqueues.
  - Returns `{ status: 'resume-enqueued', newJobId }`.
- `POST /download/:id/cancel`
  - Sets cancel markers and best-effort removes queued job.
  - Returns `{ status: 'cancelled', id }`.
- `POST /download/:id/merge-partial`
  - Partial merge helper for `durl-byte-range` manifests.

### 6) Control Signals and Coordination

- Stop:
  - Redis key/channel: `job:stop:<id>`.
  - Filesystem fallback: `data/<id>.stop`.
- Cancel:
  - Redis key/channel: `job:cancel:<id>`.
  - Filesystem marker: `data/<id>.cancel`.
- Part progress:
  - Redis hash: `job:parts:<id>`.
  - Fields like:
    - `totalExpectedBytes`
    - `part:<n>:state`, `part:<n>:expectedBytes`, `part:<n>:downloadedBytes`
    - `part:audio:<n>:...` for audio parts

### 7) Filesystem Layout

- `data/`
  - `bilibili/<safeTitle>/<bvid>-<jobId>.mp4` (final outputs)
  - `<jobId>.cancel`, `<jobId>.stop` (control markers)
  - `<jobId>-video`, `<jobId>-audio` (temporary single-stream artifacts)
  - `<jobId>/manifest.json` (strategy + part metadata)
  - `<jobId>/parts/part-<i>.bin`, `audio-part-<i>.bin`
  - `jobs/<jobId>.json` (append-only history events)

### 8) Core Modules and Owners

- API layer:
  - [`src/download/download.module.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/download/download.module.ts)
  - [`src/download/download.controller.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/download/download.controller.ts)
  - [`src/download/download.service.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/download/download.service.ts)
- Worker layer:
  - [`src/worker/worker.module.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/worker.module.ts)
  - [`src/worker/worker.processor.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/worker.processor.ts)
  - [`src/worker/parts.processor.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/parts.processor.ts)
- Integrations/utilities:
  - [`src/playurl/playurl.service.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/playurl/playurl.service.ts)
  - [`src/ffmpeg/ffmpeg.service.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/ffmpeg/ffmpeg.service.ts)
  - [`src/utils/resume-download.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/utils/resume-download.ts)
  - [`src/jobs/job-history.service.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/jobs/job-history.service.ts)

### 9) Configuration Source

- Primary human-readable config: [`config/config.yaml`](/Users/danielpham/sync-workspace/05_Stories/video-creator/config/config.yaml).
- Loaded via [`src/config/config.loader.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/config/config.loader.ts).
- Important keys:
  - `redis.url`
  - `download.dataDir`
  - `download.partSizeBytes` (interpreted as MB in worker code)
  - `download.retryCount`, `download.retryBackoffMs`
  - `download.resumeEnabled`
  - `ffmpeg.path`
  - `proxy.http`, `proxy.https`

### 10) Known Invariants (for AI edits)

- Queue names are fixed: `downloads`, `download-parts`.
- Final output naming: `<bvid>-<jobId>.mp4`.
- API can run without worker in-process; worker role is gated by `WORKER=true`.
- `POST /download` currently requires `url` by DTO, even though controller also accepts prefilled `bvid` in payload internals.
- `GET /download/status/:id` is the canonical status endpoint.
