## Plan: Tải video Bilibili — dịch vụ NestJS có thể scale

 Mục tiêu: Xây một service REST API và command-line tương ứng bằng `NestJS` (`video-downloader-api`) để nhận `BV`/URL, lấy `cid` và `playurl` từ Bilibili, tải các luồng (DASH/HLS), merge bằng `ffmpeg`, và lưu kết quả cục bộ (local). Thiết kế để dễ scale (workers queue) và an toàn (cookie handling, rate limiting).

Lưu ý: ứng dụng sẽ được tạo và lưu trực tiếp trong workspace hiện tại (`/Users/danielpham/sync-workspace/05_Stories/video-creator`). Không tạo thư mục con mới.

**Kiến trúc tổng quan**
- Gateway/API (NestJS Controller) — endpoints: `POST /download` (body: bvid|url, opts), `GET /status/:id`, `GET /result/:id`.
- Modules chính:
  - `PlayurlModule` (PlayurlService): gọi `api.bilibili.com/x/player/pagelist` và `x/player/playurl` để lấy `dash`/`durl`.
  - `DownloadModule` (DownloadService): tải file segments hoặc baseUrl, quản lý headers/cookies.
  - `FFmpegModule` (FfmpegService): merge video+audio, transcode nếu cần (thru `fluent-ffmpeg` or child_process invoking `ffmpeg`).
  - `AuthModule` / `CookieModule`: an toàn lưu tạm cookies (do NOT persist secrets by default), helper để load `cookies.txt` or accept cookies via request.
  - `QueueModule`: worker queue (Bull + Redis) để xử lý tải background, retries, timeouts.
  - `StorageModule`: local filesystem adapter (local-only storage).

**Luồng dữ liệu (high level)**
1. Client POST `/download` với `bvid` hoặc URL + optional `cookies` (or reference to user's stored session).
2. API tạo job trong queue, trả về `jobId`.
3. Worker lấy job, gọi PlayurlService để lấy `dash`/`durl`.
4. Nếu `dash` → worker tải `video.baseUrl` và `audio.baseUrl` (hoặc để `ffmpeg` stream trực tiếp), dùng `FfmpegService` để merge → lưu qua `StorageModule`.
5. Nếu `durl` → tải trực tiếp.
6. Cập nhật status, publish result URL when done.

**Tech stack & deps (Node/Nest)**
- Node 20+ / pnpm or npm
- NestJS CLI: `@nestjs/cli`
- HTTP client: `axios`
- Queue: `@nestjs/bull` + `bull` + `ioredis`
- FFmpeg: `fluent-ffmpeg` (or call `ffmpeg` binary). Provide `ffmpeg` via system package or `ffmpeg-static`.
 - Storage: local filesystem (no S3/MinIO required)
- Utils: `class-validator`, `class-transformer`, `dotenv`, `pino`/logger

Install example (scaffold in current workspace root, using pnpm):
```bash
# ensure corepack (Node 20+) and enable pnpm
corepack enable
pnpm add -g @nestjs/cli
# scaffold Nest project into current directory using pnpm
nest new . --package-manager pnpm

# then install deps with pnpm
 pnpm add axios fluent-ffmpeg ffmpeg-static bull @nestjs/bull ioredis class-validator class-transformer
```

**Endpoints (minimal)**
- POST /download
  - body: { "bvid": "BV..." } OR { "url": "https://..." }
  - optional: { "cookies": "(Netscape cookies text)" } or `cookiesId` referencing stored session
  - response: { jobId, statusUrl }
- GET /status/:id
  - response: { status, progress, errors }
- GET /result/:id
  - redirect or return signed URL to stored file

**Error handling & edge cases**
- DRM: detect when playurl returns license/workflow; mark job failed with `DRM_PROTECTED` (cannot decrypt).
- Auth/paywall: require cookies; support `--cookies-from-browser` fallback for manual runs.
- Expiring CDN URLs: fetch and stream to `ffmpeg` quickly; prefer server-side streaming to avoid storing many segments.

**Security & privacy**
- Never log full cookies. Store cookies encrypted if persisted; prefer transient sessions.
- Rate limit API; require API key for public usage.

**Deployment (Docker)**
- The app will run in Docker containers and orchestrated with `docker-compose`. Services:
  - `api` — NestJS HTTP server
  - `worker` — NestJS worker process (Bull consumer)
  - `redis` — Bull backend
  - `storage` — local filesystem for results (no S3/MinIO required)
  - `ffmpeg` is required inside the `api`/`worker` images (use `ffmpeg` system package or `ffmpeg-static`)

Create a `Dockerfile` (example):
```dockerfile
FROM node:20-bullseye-slim
WORKDIR /usr/src/app
COPY package.json pnpm-lock.yaml ./
RUN corepack enable && corepack prepare pnpm@latest --activate
RUN pnpm install --frozen-lockfile --prod
COPY . .
RUN pnpm build
RUN apt-get update && apt-get install -y ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
CMD ["node", "dist/main.js"]
```

Create `docker-compose.yml` (dev-friendly example with bind mounts):
```yaml
version: '3.8'
services:
  redis:
    image: redis:7
    restart: unless-stopped

  api:
    build:
      context: .
      target: dev
    environment:
      - NODE_ENV=development
    ports:
      - "3000:3000"
    depends_on:
      - redis
    volumes:
      - ./:/usr/src/app:cached
      - /usr/src/app/node_modules
    command: pnpm run start:dev

  worker:
    build:
      context: .
      target: dev
    command: pnpm run start:worker
    depends_on:
      - redis
    environment:
      - NODE_ENV=development
    volumes:
      - ./:/usr/src/app:cached
      - /usr/src/app/node_modules

  # MinIO removed — using local filesystem for storage

volumes:
  node_modules: {}
```

Notes for dev setup:
- The `Dockerfile` can define a `dev` build stage that installs dependencies but doesn't run `pnpm build`, e.g.:
```dockerfile
FROM node:20-bullseye-slim AS base
WORKDIR /usr/src/app
COPY package.json pnpm-lock.yaml ./
RUN corepack enable && corepack prepare pnpm@latest --activate
RUN pnpm install --frozen-lockfile

FROM base AS dev
COPY . .
CMD ["pnpm", "run", "start:dev"]
```

- Bind-mounting the project avoids rebuilding the image while developing. The `node_modules` anonymous volume prevents host/node_modules conflicts.
- Run `docker compose up --build` once to populate `node_modules` inside the container, then `${command: pnpm run start:dev}` will pick up code changes thanks to Nest's watch mode.

**Milestones / Steps (implementation plan)**
1. Scaffold NestJS project (`video-downloader-api`) and basic health endpoint.
2. Implement `PlayurlService` (pagelist + playurl calls, JSON parsing). Add unit tests.
3. Implement `DownloadService` to fetch baseUrl/durl with headers/cookies.
4. Implement `FfmpegService` to merge video+audio (use `ffmpeg-static` or system binary). Add integration test using public BV.
5. Add `QueueModule` (Bull) and migrate processing to workers.
6. Add `StorageModule` (local filesystem adapter) and result endpoints.
7. Add cookie handling docs and secure storage option.
8. Add README, Dockerfile, and CI tests.

**Files to create (initial)**
- `src/app.module.ts`
- `src/download/download.module.ts`
- `src/download/download.controller.ts`
- `src/download/download.service.ts` (queue producer)
- `src/worker/worker.module.ts`
- `src/worker/worker.processor.ts` (job handler, uses PlayurlService, DownloadService, FfmpegService)
- `src/playurl/playurl.service.ts`
- `src/ffmpeg/ffmpeg.service.ts`
- `src/storage/storage.module.ts` + adapters
- `scripts/download_bilibili.sh` (fallback CLI)
- `README.md` (usage, cookie export instructions)

**Commands (Docker / Compose)**
Build and start services (development):
```bash
# build images and start services in background
docker compose up --build -d

# view logs
docker compose logs -f api
docker compose logs -f worker

# stop
docker compose down
```

Run a one-off worker or shell for debugging:
```bash
docker compose run --rm api pnpm test
docker compose run --rm worker pnpm start:worker
```

**Cookie export for Chrome (short)**
- Recommend user export `cookies.txt` via extension (Get cookies.txt) or provide `SESSDATA` + `bili_jct` values through secure UI; include example of Netscape format in `README.md`.

**Notes**
 - This plan favors reliability and scaling: long downloads handled by workers, results stored locally on the filesystem, API remains responsive.
- DRM content cannot be decrypted; service will detect and report.

---

Ghi chú: tôi đã cấu hình scope là local-only (no S3/MinIO). Nếu muốn thêm S3/MinIO later, tôi có thể add a storage adapter.
