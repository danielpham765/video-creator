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
  - Emits stop/cancel signals: the resume controller now writes a Redis stop key `job:stop:<id>` (with a short TTL) and waits for the worker to acknowledge `stopped` before requeuing; as a fallback the API may write filesystem stop markers. The cancel endpoint still writes `data/<id>.cancel` (recommended to convert to Redis for cross-host durability).

- **Worker process** (`node dist/worker.js`, see `WorkerProcessor`)
  - Subscribes to the `downloads` queue.
  - For each job:
    - Resolves Bilibili `cid` and `playurl`.
    - Chooses between DASH (`dash`) vs fallback `durl` download strategy.
    - Uses `resumeDownload()` to fetch stream(s) robustly with resume support. The worker now listens for stop requests via a duplicated Redis client (pub/sub) and falls back to polling the `job:stop:<id>` key; when a stop is received the worker sets a `stopRequested` flag that is passed into `resumeDownload()` so downloads abort cooperatively. The worker cleans up the subscriber (unsubscribe/disconnect) and removes the stop key on acknowledgement.
    - Invokes `FfmpegService.merge()` when separate audio/video streams exist.
    - Writes progress and history events.
    - Honors stop/cancel signals cooperatively.

### Module and service map

- **`DownloadModule`** (`src/download/download.module.ts`)
  - Imports:
    - `BullModule.registerQueue({ name: 'downloads' })`
    ```markdown
    ## Video Downloader API – Developer Architecture

    ### High-level overview

    - **Purpose**: Background-friendly Bilibili video downloader. The HTTP API accepts a `bvid` (and optional cookies), resolves playback URLs via Bilibili APIs, downloads streams reliably (with resume/cancel/stop), merges audio+video with `ffmpeg`, and writes result files to a local `data/` directory.
    - **Style**: NestJS modules + Bull queue workers. The HTTP layer is thin; almost all work happens in a Bull processor that can run in a separate worker process/container.
    - **Persistence**: No DB. Job state is in Bull/Redis; job history and marker files live under `data/`.

    ### Recent changes (Feb 2026)

    - `POST /download` now validates request bodies with a DTO and is visible in Swagger (see `src/download/dto/create-download.dto.ts`).
    - App bootstrap moved to `src/main.ts`: global `ValidationPipe`, global `LoggingInterceptor`, and Swagger/OpenAPI configured at `/api` and `/docs`.
    - `AppModule` is a dynamic registrar (`AppModule.register()`) so the same image can run as the API or a worker. The `WorkerModule` is only loaded when `WORKER=true`.
    - `BullModule.forRootAsync()` has been added into the dynamic `AppModule.register()` so the API can enqueue jobs even if the worker is separate.
    - `resumeDownload()` accepts an `options.logger` now and worker code passes Nest `Logger` (so download progress appears in Nest logs).
    - `DownloadService.enqueue()` now maps queue/Redis enqueue errors to HTTP 503 so API clients see a clear "Queue service unavailable" response when Redis is down.
    - A global `LoggingInterceptor` logs requests/responses; it required adding `rxjs` as a dependency.
    - Docker/docker-compose changes: development mounts were adjusted to avoid hiding container-installed `node_modules` (use an anonymous or named volume for `node_modules`), and `docker-compose` is used to scale worker replicas for parallel processing.

    ### Core runtime processes

    - **API process** (`nest start` / `start:dev`)
      - Exposes REST endpoints under `/download`.
      - Enqueues background jobs into the `downloads` Bull queue.
      - Reads job status/result from Bull and from job-history JSON files.
      - Emits stop/cancel signals: the resume controller writes a Redis stop key `job:stop:<id>` (with a short TTL) and waits for the worker to acknowledge `stopped` before requeuing; as a fallback the API may write filesystem stop markers. The cancel endpoint still writes `data/<id>.cancel` (recommended to convert to Redis for cross-host durability).

    - **Worker process** (`node dist/worker.js`, see `WorkerProcessor`)
      - Subscribes to the `downloads` queue.
      - For each job:
        - Resolves Bilibili `cid` and `playurl`.
        - Chooses between DASH (`dash`) vs fallback `durl` download strategy.
        - Uses `resumeDownload()` to fetch stream(s) robustly with resume support. The worker listens for stop requests via a duplicated Redis client (pub/sub) and falls back to polling the `job:stop:<id>` key; when a stop is received the worker sets a `stopRequested` flag that is passed into `resumeDownload()` so downloads abort cooperatively. The worker cleans up the subscriber (unsubscribe/disconnect) and removes the stop key on acknowledgement.
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
          - Body: `CreateDownloadDto` (`url` required, `title` optional) — validated by `class-validator`.
          - Calls `DownloadService.enqueue(body)` which enqueues to `downloads` queue.
          - Returns `{ jobId }` for client-side polling.
        - `GET /download/status/:id`
          - Looks up a Bull job by id and reads job-related keys from Redis to build a progress view.
          - Returns `state`, `progress`, `failedReason`, `result`, and `history` (from job history JSON).
        - `POST /download/:id/resume` and `POST /download/:id/cancel` — see implementation notes in `src/download/download.controller.ts` for cooperative stop/resume and cancel semantics.

    - **`DownloadService`** (`src/download/download.service.ts`)
      - Thin abstraction over Bull: injects `Queue` for `downloads` and exposes `enqueue(payload)` which catches enqueue errors and throws a 503 when Redis/queue is unavailable.

    - **`PlayurlModule` / `PlayurlService`** (`src/playurl`)
      - `getCidFromBvid(bvid)` and `getPlayurl(bvid, cid, cookies?)` — Bilibili API wrappers used by the worker.

    - **`FfmpegModule` / `FfmpegService`** (`src/ffmpeg`)
      - Wraps `fluent-ffmpeg` for merging video+audio (`-c copy` by default).

    - **`JobsModule` / `JobHistoryService`** (`src/jobs`)
      - Persists lifecycle events to `data/jobs/<jobId>.json` and provides `appendEvent()` / `getHistory()` helpers used by controllers and workers.

    - **`WorkerProcessor`** (`src/worker/worker.processor.ts`)
      - `@Processor('downloads')` handles job lifecycle: resolving, downloading (via `resumeDownload()`), merging, and appending history. Worker handles stop/cancel keys and passes Nest `Logger` into `resumeDownload()` so progress logs appear in container logs.

    - **`resumeDownload` util** (`src/utils/resume-download.ts`)
      - Streams HTTP content to disk with Range/resume support, cooperative stop/cancel via a function or marker file, and accepts an `options.logger` to emit progress using Nest `Logger`.

    ### Data layout on disk

    - `data/`
      - `<bvid>-<jobId>.mp4` – final outputs.
      - `<jobId>-video`, `<jobId>-audio` – transient DASH download files.
      - `<jobId>.cancel` – cancel marker, written by controller; read by worker and `resumeDownload`.
      - `<jobId>.stop` – stop marker alternative to Redis pub/sub.
      - `jobs/` — `<jobId>.json` – append-only array of history events.

    ### Queue & Redis integration

    - **Queue name**: `downloads` (registered via `BullModule.registerQueue({ name: 'downloads' })`).
    - **Stop signaling**: Redis key `job:stop:<id>` (workers prefer pub/sub and fall back to polling); workers duplicate the Bull client for a subscriber and set `stopRequested` flag.
    - **Cancel signaling**: filesystem marker `data/<id>.cancel` (recommended future improvement: migrate to Redis `job:cancel:<id>` + pub/sub).

    ### Error handling and job states (practical view)

    - Common states: `queued`, `resolving`, `downloading-video`, `downloading-audio`, `merging`, `finished`, `failed`, `cancelled`, `stopped`, `resume-requested`, etc.

    ### Extensibility notes

    - New sources: implement resolvers like `PlayurlService` and branch in `WorkerProcessor`.
    - Stronger API contracts: DTOs + `@nestjs/swagger` already in place for `POST /download`.

    ### How to run locally (api vs worker)

    Run the full stack (api + worker + redis):

    ```bash
    docker compose up --build -d
    ```

    Run only the API (same image, no workers running in-process):

    ```bash
    # API-only (recommended for local dev)
    docker compose up --build -d api
    ```

    Run a worker replica (sets `WORKER=true` so the dynamic module loads `WorkerModule`):

    ```bash
    # start one worker
    docker compose up --build -d --no-deps --scale worker=1 worker

    # scale to N workers for parallelism
    docker compose up -d --no-deps --scale worker=4 worker
    ```

    Notes:
    - The runtime checks `WORKER=true` to decide whether to register `WorkerModule` — this allows the same container image to be launched as API or worker.
    - If Redis is not available, `POST /download` will return 503; start `redis` first in `docker compose`.

    ### Logs & verifying parallelism

    To attribute log lines to particular worker containers, capture logs with container prefixes (don't use `--no-log-prefix`):

    ```bash
    docker compose logs --tail=200 worker
    ```

    Or fetch per-container logs after `docker compose ps` so you can see which replica handled each `download` / `download-parts` job.

    If you want, I can fetch and parse prefixed worker logs now to annotate which worker processed each part.

    ## Flow diagram

    ```mermaid
    flowchart LR
      Client[Client\n(HTTP request)] --> API[API / DownloadController]
      API --> Queue[Bull queue: downloads]
      API -->|SET `job:stop:<id>` / write stop marker| Redis[Redis (pub/sub & keys)]
      API -->|write `data/<id>.cancel`| Disk[Disk (`data/`)]

      Queue --> Worker[WorkerProcessor]
      Redis -->|pub/sub or GET| Worker
      Worker -->|calls| Resume[`resumeDownload()` util]
      Resume -->|writes partials| Disk
      Worker -->|on DASH: download segments| Disk
      Worker -->|on merge| Ffmpeg[FfmpegService (merge)]
      Ffmpeg -->|final file| Disk
      Worker -->|append events| Jobs[JobHistory (`data/jobs/<id>.json`)]

      API -->|requeue after stop| Queue

      classDef infra fill:#f9f,stroke:#333,stroke-width:1px;
      class Redis,Disk,Jobs infra;
    ```

    This diagram shows the high-level message flow: the API enqueues jobs and signals stop/cancel via Redis or disk markers; the `WorkerProcessor` subscribes to Redis (or polls) and passes a `stopRequested()` function into `resumeDownload()` so downloads can abort cooperatively; after downloads finish the worker runs `FfmpegService.merge()` and records history in `data/jobs/`.
  - `cancelled`

  - `stopped`, `stop-requested`, `stop-timeout`, `resume-requested`, `requeueing`, `requeue-failed`
