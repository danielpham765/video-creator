# video-downloader-api

Dev: uses Docker + docker-compose with bind mounts to avoid rebuilds.

Quick start (macOS, Node 20+):

```bash
# enable corepack for pnpm
corepack enable

# build and start dev containers
docker compose up --build -d

# view logs (info)
docker compose logs -f api
docker compose logs -f worker

# view logs (debug)
make logs api
make logs worker

```

