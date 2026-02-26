# video-downloader-api

Dev: uses Docker + docker-compose with bind mounts to avoid rebuilds.

Quick start (macOS, Node 20+):

```bash
# enable corepack for pnpm
corepack enable

# build and start dev containers
docker compose up --build -d

# view API logs
docker compose logs -f api
```

API endpoints:
- `POST /download` { bvid | url, cookies }
- `GET /status/:id`
- `GET /result/:id`

See `/plan/PLAN_nestjs.md` for detailed plan and dev notes.
