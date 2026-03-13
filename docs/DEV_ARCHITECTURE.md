## Video Downloader API - Developer Architecture

Last updated: 2026-03-08

### High-level overview

- Purpose: background-friendly multi-source downloader. API receives download requests, worker resolves playable URLs via platform handlers, downloads, merges/transcodes, and stores final files in `video-result/`.
- Stack: NestJS + Bull + Redis + Postgres + FFmpeg.
- Persistence:
  - Bull/Redis for in-flight queue state.
  - Postgres (`download_job_archive`) for terminal master-job archival (append-safe via `archive_key = <run_id>:<job_id>`).
  - Filesystem for artifacts/history (`data/`, `logs/`).

### Runtime topology

- API process (`WORKER` unset/false):
  - Exposes `/download` endpoints.
  - Enqueues master jobs to queue `downloads`.
  - Reads status from Bull + Redis + history files.
- Worker process (`WORKER=true`):
  - Loads `WorkerModule`.
  - Consumes queue `downloads` (master jobs) and `download-parts` (part jobs).

### Queue model

- `downloads`:
  - One master job per request.
  - Processor: `WorkerProcessor`.
- `download-parts`:
  - Part jobs for `dash-segmented`, `dash-byte-range`, and `durl-byte-range`.
  - Processor: `PartsProcessor`.

### Module and service map

- `DownloadModule`:
  - wires `DownloadController` and `DownloadService`.
  - registers queue `downloads`.
- `SourceModule`:
  - resolves source URL/page into normalized streams via Bilibili/YouTube/Generic handlers.
- `MediaPlannerService`:
  - maps requested `media` (`both|video|audio`) to effective mode with fallback reason when needed.
- `WorkerProcessor`:
  - handles master `downloads` jobs (strategy select, merge, history).
- `PartsProcessor`:
  - handles `download-parts` jobs (segments/ranges).
- `FfmpegService`:
  - merges streams and concat-lists.
- `JobHistoryService`:
  - persists lifecycle events under `data/jobs/<jobId>.json`.

### API routes

- `POST /download`
  - Body DTO: `CreateDownloadDto` (`url` required, `title?`, `platform?`, `media?`).
  - Controller enqueues payload with normalized `{ platform, vid, mediaRequested, url, title }`.
- `GET /download/status/:id`
  - Returns job `state`, `progress`, `failedReason`, `result`, `history`.
  - Adds parts summary from Redis hash `job:parts:<id>`.
  - Falls back to latest Postgres archive row by `job_id` when Bull misses.
- `POST /download/:id/resume`
  - Sends stop signal (`job:stop:<id>`), waits for `stopped`, then enqueues new job.
  - Falls back to latest archived payload by `job_id` in Postgres when source job is not in Bull.
- `POST /download/:id/cancel`
  - Sets Redis cancel key/channel (`job:cancel:<id>`), best-effort removes queued job.
  - Does not create `/data/<id>.cancel` marker from API.
- `POST /download/:id/merge-partial`
  - Partial merge helper for `durl-byte-range` manifests.

### Source and auth inputs

- Cookies are loaded by worker from `config/cookies/<source>.json`.
- Request body does not carry cookies.
- Supported cookie formats in file:
  - browser export array (`[{name,value}, ...]`)
  - key/value object
  - plain cookie string

### Video quality policy

- Worker enforces preferred quality with lower fallback by source `qn` code.
- Config:
  - `download.preferVideoQuality` (default `1080p`)
  - Allowed: `2160p`, `1440p`, `1080p`, `720p`, `480p`, `360p`
- Selection rule:
  - pick preferred quality first
  - fallback to lower quality if preferred is unavailable
  - minimum fallback is `720p`, except when preferred is `480p` or `360p`
- If resolved quality is below the accepted minimum, job fails immediately.
- For DASH, worker selects the highest video track in accepted range `[minAcceptableQn, preferredQn]`.

### Title and output naming flow

- If request has `title`: use it.
- If request has no `title`:
  - worker fetches source title via Bilibili view API.
  - if title is not Vietnamese/English script, worker attempts translation to Vietnamese.
- Folder title normalization:
  - Vietnamese/Latin: remove accents, lowercase, spaces -> `-`, trim duplicates.
  - Chinese/CJK: keep original (sanitized for path-invalid chars).
- Final path:
  - `video-result/<platform>/<normalizedTitle>/<vid>-<jobId>.<ext>`

### Strategy selection (current order)

Selection logic is in `src/worker/worker.processor.ts` and follows this order:

1. `dash-segmented`
2. `dash-byte-range`
3. `durl-byte-range`
4. `dash-single`
5. `durl` (single file)

#### 1) `dash-segmented`

- Condition:
  - `play.data.dash` has video/audio URLs.
  - manifest detection succeeds (`#EXTM3U` or `<MPD>`).
- Flow:
  - parse manifests -> segment URLs.
  - group segments into parts.
  - enqueue `download-parts` for video/audio.
  - wait part completion, concat parts, merge A/V with ffmpeg.
- Notes:
  - best for long content.
  - uses part-job concurrency controls.

#### 2) `dash-byte-range`

- Condition:
  - `play.data.dash` has video/audio URLs.
  - segmented manifest mode is not applicable.
  - both DASH URLs support true range:
    - HEAD has `content-length`
    - `accept-ranges` contains `bytes`
    - size >= `2 * partSizeBytes`
    - probe GET `Range: bytes=0-1` returns `206`
- Flow:
  - split both video/audio streams into byte-range parts.
  - enqueue `download-parts` with role `video` and `audio`.
  - concat video/audio parts separately, then ffmpeg merge A/V.
- Notes:
  - keeps DASH quality selection while still enabling parallel parts.

#### 3) `durl-byte-range`

- Condition:
  - `durl` URL exists.
  - origin supports true range:
    - HEAD has `content-length`.
    - `accept-ranges` contains `bytes`.
    - size >= `2 * partSizeBytes`.
    - probe GET `Range: bytes=0-1` returns `206`.
- Flow:
  - split by byte ranges.
  - enqueue `download-parts`.
  - concat parts into final media file.
- Notes:
  - preferred over `dash-single` when applicable.

#### 4) `dash-single`

- Condition:
  - DASH URLs available.
  - segmented path not applicable.
  - and no successful `dash-byte-range` / `durl-byte-range` branch.
- Flow:
  - master downloads full video stream + full audio stream.
  - ffmpeg merge.
- Notes:
  - has byte-based debug progress logging.
  - resume support via `Range` when partial temp files exist.

#### 5) `durl` (single file)

- Condition:
  - `durl` URL exists.
  - byte-range parallel not applicable.
- Flow:
  - master downloads single URL via `resumeDownload()`.
- Notes:
  - also has byte-based debug progress logging.

### Progress accounting

- Total expected bytes are stored in Redis hash `job:parts:<jobId>`:
  - `totalExpectedBytes`
  - `part:<i>:expectedBytes`, `part:<i>:downloadedBytes`, `part:<i>:state`
  - `part:audio:<i>:...` for audio parts
- Master jobs map byte progress to job progress ranges (for example segmented phases around 30-79).

### Concurrency and limiter model

#### Master job concurrency (`downloads`)

- Controlled by:
  - `worker.concurrency`
  - `download.singleMaxConcurrentDownloads`
- Effective master slot count in worker:
  - `min(worker.concurrency, download.singleMaxConcurrentDownloads)`

#### Part job concurrency (`download-parts`)

- Per worker process limit:
  - `download.parallelMaxConcurrentDownloads`
- Global limit across all replicas/processes:
  - `download.globalParallelMaxConcurrentDownloads`
  - Redis ZSET semaphore key: `semaphore:downloads:parallel:global`

#### Single-download global limit

- For `dash-single` and `durl` single path, worker acquires global permit from:
  - `download.globalSingleMaxConcurrentDownloads`
  - Redis ZSET semaphore key: `semaphore:downloads:single:global`

#### Lease-based permit behavior

- `download.globalLimiterLeaseMs`:
  - permit lease TTL window.
  - expired tokens are reclaimed automatically.
- `download.globalLimiterWaitMs`:
  - retry interval while waiting for permit.
- Tokens are periodically renewed; on completion they are released.

### Stop/cancel behavior

- Stop:
  - Redis key/channel `job:stop:<id>`.
- Cancel:
  - Redis key/channel `job:cancel:<id>`.
- Worker and `resumeDownload()` check these signals cooperatively.

### Logging architecture

Logger: `FileLoggerService` (used as Nest logger in `main.ts`).

- Console output:
  - only `info` level is printed.
- Service log folders:
  - API: `logs/api`
  - Worker instance: `logs/worker/<worker-label>`
  - Worker aggregate: `logs/worker/app.*.log`
- Per-folder files:
  - `app.info.log`: info + warn + error
  - `app.warn.log`: warn + error
  - `app.error.log`: error
  - `app.debug.log`: debug + info + warn + error
- Worker line prefix:
  - `worker-<index>  | ...`
  - If numeric worker index is not available, logger uses last 2 chars of host/process label.

### Log rotation and archive

- Rotation is checked periodically; if any managed log file exceeds configured size, logger rotates all managed log files together.
- Archive file name format: `yyyy-mm-dd_hh-mm-ss.zip` (with suffix `_n` if collision).
- Destinations:
  - local (`logs/archived`)
  - S3 (configurable bucket/prefix/credentials/endpoint)
- Retention:
  - local: max files and optional retention days.
  - S3: retention days cleanup.

### Key config fields (`config/config.default.yaml` + `config/config.<source>.yaml`)

- `worker.concurrency`
- `download.partSizeBytes` (interpreted as MB)
- `download.parallelMaxConcurrentDownloads`
- `download.singleMaxConcurrentDownloads`
- `download.globalParallelMaxConcurrentDownloads`
- `download.globalSingleMaxConcurrentDownloads`
- `download.globalLimiterLeaseMs`
- `download.globalLimiterWaitMs`
- `download.retryCount`
- `download.resumeEnabled`
- `logging.level`
- `logging.rotate.*`

### Filesystem layout

- `data/`
  - `<jobId>/manifest.json`
  - `<jobId>/parts/part-<i>.bin`, `audio-part-<i>.bin`
  - `jobs/<jobId>.json`
- `video-result/`
  - `<platform>/<normalizedTitle>/<vid>-<jobId>.<ext>`
- `logs/`
  - `api/app.*.log`
  - `worker/<worker-label>/app.*.log`
  - `worker/app.*.log` (aggregate workers)
  - `archived/*.zip`

### Local run notes

- Full stack:
  - `docker compose up --build -d`
- API only:
  - `docker compose up --build -d api`
- Scale workers:
  - `docker compose up -d --no-deps --scale worker=4 worker`

### Archive identity note

- Bull `job_id` is Redis-lifecycle scoped and can reset after Redis cleanup/restart.
- Archive rows are uniquely keyed by `archive_key = <run_id>:<job_id>`.
- Id-only API lookup (`/download/status/:id`, `/download/:id/resume`) uses the newest archived row for that `job_id`.
