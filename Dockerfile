FROM python:3.12-slim-bookworm

# System dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gosu curl ca-certificates gnupg bash \
        build-essential && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# uv — fast Python package manager
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/

WORKDIR /app

# Environment: bytecode compilation, deterministic builds
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install dependencies first (Docker layer caching)
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy application code
COPY . .

# Apply mem0 library patches (must run after uv sync)
RUN bash mem0bot/patches/apply_patches.sh

# Entrypoint handles user creation, credential setup, and startup
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
