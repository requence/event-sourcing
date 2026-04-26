# ── Stage 1: Install dependencies ─────────────────────────────────────
FROM oven/bun:1 AS deps

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# ── Stage 2: Build the static site ───────────────────────────────────
FROM oven/bun:1 AS build

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

RUN bun run build

# ── Stage 3: Serve with nginx ────────────────────────────────────────
FROM nginx:alpine AS runtime

RUN apk add --no-cache curl

COPY --from=build /app/dist /usr/share/nginx/html

# SPA-friendly fallback & caching
COPY <<'EOF' /etc/nginx/conf.d/default.conf
server {
    listen       80;
    server_name  _;
    root         /usr/share/nginx/html;
    index        index.html;

    # Cache static assets aggressively
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff2?)$ {
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # Serve pre-rendered pages; fall back to index.html
    location / {
        try_files $uri $uri/index.html $uri.html /index.html;
    }
}
EOF

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
  CMD curl -f http://localhost/ || exit 1

EXPOSE 80
