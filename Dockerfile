FROM node:lts-alpine AS builder
WORKDIR /app

RUN corepack enable && corepack prepare pnpm@latest --activate
    
# Add necessary tools for private repo cloning
RUN apk add --no-cache git openssh-client

# Copy package files
COPY package.json pnpm-lock.yaml ./
    
# Install dependencies with GitHub token securely
RUN --mount=type=secret,id=gh_token \
  GH_TOKEN=$(cat /run/secrets/gh_token) && \
  export GITHUB_TOKEN=$GH_TOKEN && \
  git config --global url."https://${GH_TOKEN}:@github.com/".insteadOf "https://github.com/" && \
  git config --global url."https://${GH_TOKEN}:@github.com/".insteadOf "git@github.com:" && \
  pnpm install --frozen-lockfile


COPY . .
RUN pnpm build

FROM node:lts-alpine
WORKDIR /app
COPY --from=builder /app /app
CMD ["pnpm", "run", "setup:kafka"]
