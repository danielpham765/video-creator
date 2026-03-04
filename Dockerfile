FROM node:20-bullseye-slim AS base
WORKDIR /usr/src/app
COPY package.json pnpm-lock.yaml* ./
RUN corepack enable && corepack prepare pnpm@latest --activate
# allow install when pnpm-lock.yaml is not present in dev environment
RUN pnpm install --no-frozen-lockfile

FROM base AS dev
COPY . .
RUN pnpm install --no-frozen-lockfile
RUN apt-get update && apt-get install -y ffmpeg ca-certificates procps python3-pip \
  && pip3 install --no-cache-dir yt-dlp \
  && rm -rf /var/lib/apt/lists/*
CMD ["pnpm", "run", "start:dev"]

FROM base AS prod
COPY . .
RUN pnpm build
RUN apt-get update && apt-get install -y ffmpeg ca-certificates procps python3-pip \
  && pip3 install --no-cache-dir yt-dlp \
  && rm -rf /var/lib/apt/lists/*
CMD ["node", "dist/main.js"]
