## Video Creator API Architecture (AI Cache)

Last updated: 2026-03-04
Source of truth for Cursor, Codex, and Antigravity AI.

### 1) Purpose

- Project: `video-creator` (NestJS + Bull + Redis + Postgres + FFmpeg).
- Goal: accept multi-platform video/page URLs (Bilibili, YouTube, generic), run background downloads, and write final media files under `result/`.
- Persistence:
  - In-flight queue/runtime state in Bull + Redis.
  - Terminal master-job archive in Postgres table `download_job_archive` (append-safe with `archive_key = <run_id>:<job_id>`).
  - Durable artifacts/history in filesystem (`data/`, `logs/`).

### 2) Runtime Topology

- API process:
  - Bootstrapped by [`src/main.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/main.ts).
  - Loads dynamic [`AppModule.register()`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/app.module.ts).
  - Exposes REST + Swagger (`/api`, `/docs`).
  - Enqueues `downloads` jobs.
- Worker process (`WORKER=true`):
  - Loads [`WorkerModule`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/worker.module.ts).
  - Consumes `downloads` and `download-parts` queues.

### 3) Queue Model

- Queue `downloads`:
  - Master/orchestration job per requested download.
  - Processor: [`WorkerProcessor`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/worker.processor.ts).
- Queue `download-parts`:
  - Sub-jobs for segmented/range part downloads.
  - Processor: [`PartsProcessor`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/worker/parts.processor.ts).

### 4) Request and Processing Flow

1. Client calls `POST /download` with `CreateDownloadDto` (`url` required, `title` optional).
2. [`DownloadController.startDownload()`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/download/download.controller.ts) accepts URL + optional `platform` and `media`, then enqueues a `downloads` job with normalized `vid`.
3. Worker resolves streams via platform handlers in `src/source/`.
   - Resolver/worker quality policy is driven by `download.preferVideoQuality` (default `1080p`) with downward fallback and source-specific stream matching.
4. Worker selects strategy in this order:
   - `dash-segmented`
   - `dash-byte-range`
   - `durl-byte-range`
   - `dash-single`
   - `durl` (single file)
5. Parts (if any) are merged, then video/audio merged by [`FfmpegService`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/ffmpeg/ffmpeg.service.ts).
6. Final output: `result/<platform>/<safeTitle>/<vid>-<jobId>.<ext>` where `<ext>` is `.mp4` or `.m4a`.
7. History events are appended to `data/jobs/<jobId>.json`.

### 5) Public API (Current)

- `POST /download`
  - Body: `{ url: string, title?: string, platform?: 'auto'|'bilibili'|'youtube'|'generic', media?: 'both'|'video'|'audio' }`
  - Response: `{ jobId }`
- `GET /download/status/:id`
  - Returns job state/progress/failure/result/history + parts summary from Redis when active.
  - Falls back to latest Postgres archive row by `job_id` when job is no longer in Bull/Redis.
- `POST /download/:id/resume`
  - Sends stop signal and re-enqueues as a new job id.
  - If source job is archived, loads latest archived payload by `job_id` and enqueues fresh Bull job.
- `POST /download/:id/cancel`
  - Sets Redis cancel signal (`job:cancel:<id>`) and best-effort removes queued job.
  - No filesystem cancel marker is created by API.
- `POST /download/:id/merge-partial`
  - Partial merge helper for byte-range manifests.

### 6) Control Signals and Coordination

- Stop:
  - Redis key/channel: `job:stop:<id>`
- Cancel:
  - Redis key/channel: `job:cancel:<id>`
- Part progress:
  - Redis hash: `job:parts:<id>`

### 7) Concurrency and Limits

- Master jobs (`downloads`) per worker process:
  - effective cap = `min(worker.concurrency, download.singleMaxConcurrentDownloads)`.
- Part jobs (`download-parts`) per worker process:
  - `download.parallelMaxConcurrentDownloads`.
- Global single-download cap (across replicas):
  - `download.globalSingleMaxConcurrentDownloads`
  - semaphore key: `semaphore:downloads:single:global`.
- Global part-download cap (across replicas):
  - `download.globalParallelMaxConcurrentDownloads`
  - semaphore key: `semaphore:downloads:parallel:global`.
- Lease/wait tuning:
  - `download.globalLimiterLeaseMs`
  - `download.globalLimiterWaitMs`

### 8) Filesystem Layout

- `result/`
  - `<platform>/<safeTitle>/<vid>-<jobId>.<ext>`
  - `<jobId>/manifest.json`
  - `<jobId>/parts/part-<i>.bin`, `audio-part-<i>.bin`
  - `jobs/<jobId>.json`
- `logs/`
  - `api/app.*.log`
  - `worker/<worker-label>/app.*.log`
  - `worker/app.*.log` (aggregated)
  - `archived/*.zip`

### 9) Logging and Rotation

- Logger implementation: [`FileLoggerService`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/common/file-logger.service.ts).
- Console prints only `info` logs.
- File fanout:
  - `app.info.log`: info + warn + error
  - `app.warn.log`: warn + error
  - `app.error.log`: error
  - `app.debug.log`: debug + info + warn + error
- Rotation:
  - Trigger: any managed file exceeds size threshold.
  - Action: zip all managed logs together, then truncate all.
  - Archive name: `yyyy-mm-dd_hh-mm-ss.zip`.
  - Destination: local (`logs/archived`) or S3.

### 10) Configuration Source

- Human-readable config: [`config/config.default.yaml`](/Users/danielpham/sync-workspace/05_Stories/video-creator/config/config.default.yaml) with source overrides in `config/config.<source>.yaml`.
- Loader: [`src/config/config.loader.ts`](/Users/danielpham/sync-workspace/05_Stories/video-creator/src/config/config.loader.ts).
- Key sections:
  - `redis.*`
  - `worker.*`
  - `download.*`
  - `archive.*`
  - `ffmpeg.*`
  - `proxy.*`
  - `logging.*`

### 11) Current Invariants

- Queue names are fixed: `downloads`, `download-parts`.
- Final file naming stays `<vid>-<jobId>.<ext>` inside normalized title folder.
- Cookies are read from `config/cookies/<source>.json` by worker.
- `GET /download/status/:id` is canonical status endpoint.
- `job_id` can be reused after Redis reset; archive identity is `archive_key` and lookup-by-id uses newest row.
