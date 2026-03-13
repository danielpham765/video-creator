# Video Creator API

NestJS + Bull + Redis + Postgres service for background video downloads (Bilibili, YouTube, and generic sources), with worker-side merge/transcode and job history tracking.

## What this project does

- Exposes HTTP endpoints to enqueue download jobs and query progress.
- Uses Bull queues for async processing:
  - `downloads` for master jobs.
  - `download-parts` for segmented/byte-range part jobs.
- Runs API and worker in separate processes (same codebase, toggled by `WORKER=true`).
- Writes intermediate artifacts/manifests to `data/` and final media files to `video-result/`.

## Prerequisites

- Docker + Docker Compose (recommended for local development).
- `pnpm` (via Corepack) if you also run commands outside containers.

## Quick start (Docker)

```bash
# from project root
corepack enable
docker compose up --build -d
```

Services started by `docker-compose.yml`:

- `api` on `http://localhost:3000`
- `worker` (2 replicas by default)
- `postgres` on `localhost:5432`
- `redis` on `localhost:6379`

## API docs and core routes

- Swagger UI:
  - `http://localhost:3000/docs`
  - `http://localhost:3000/api`

- Core endpoints:
  - `POST /download`
  - `GET /download/status/:id`
  - `POST /download/:id/resume`
  - `POST /download/:id/cancel`
  - `POST /download/:id/merge-partial`

Example request:

```bash
curl -X POST http://localhost:3000/download \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "https://www.bilibili.com/video/BV1xx411c7mD",
    "platform": "auto",
    "media": "both",
    "engine": "auto"
  }'
```

## Logs

- Container logs:
  - `docker compose logs -f api`
  - `docker compose logs -f worker`
- File logs (inside mounted workspace):
  - API: `logs/api/app.*.log`
  - Worker aggregate: `logs/worker/app.*.log`
  - Worker instance: `logs/worker/<worker-label>/app.*.log`
- Helper commands:
  - `make api`
  - `make worker`

## Important directories

- `config/` runtime YAML config (`config.default.yaml` + source overrides).
- `data/` manifests, part files, and job history (`data/jobs/<jobId>.json`).
- `video-result/` final output files by platform/title.
- `docs/` architecture and developer notes.
- `test/manual/` manual integration/debug scripts (excluded from Jest runs).

## Related docs

- [Developer architecture](docs/DEV_ARCHITECTURE.md)
- [Architecture cache/summary](docs/ARCHITECTURE.md)
- [Scripts notes](scripts/README.md)
- [Manual test scripts](test/manual/README.md)
