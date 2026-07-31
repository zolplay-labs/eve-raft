FROM node:24-slim

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"

RUN corepack enable

WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json tsconfig.build.json prettier.config.mjs ./
COPY src src
COPY fixtures/standalone fixtures/standalone

RUN pnpm install --frozen-lockfile
RUN pnpm build
RUN pnpm install --offline --frozen-lockfile
RUN pnpm --dir fixtures/standalone build

WORKDIR /app/fixtures/standalone

CMD ["pnpm", "exec", "eve-raft", "start", "--data-dir", "/data", "--", "pnpm", "exec", "eve", "start"]
