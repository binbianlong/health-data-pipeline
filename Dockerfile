# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.13-slim@sha256:6771159cd4fa5d9bba1258caf0b82e6b73458c694d178ad97c5e925c2d0e1a91
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.8.14@sha256:f3660c56d5b08d6c516360981bedc439f499b9bf37f46a216018da3777a74011

# hadolint ignore=DL3006
FROM ${UV_IMAGE} AS uv

# hadolint ignore=DL3006
FROM ${PYTHON_IMAGE} AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /app

FROM base AS deps

COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM deps AS test

COPY src ./src
COPY tests ./tests
COPY schemas ./schemas
COPY infra ./infra
COPY cloudbuild.yaml ./cloudbuild.yaml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-groups
RUN uv run ruff format --check . \
    && uv run ruff check . \
    && uv run mypy src tests \
    && uv run pytest

FROM deps AS runtime-build

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# hadolint ignore=DL3006
FROM ${PYTHON_IMAGE} AS runtime

ENV PATH=/app/.venv/bin:$PATH \
    PORT=8080 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /nonexistent app

WORKDIR /app
COPY --from=runtime-build --chown=app:app /app/.venv /app/.venv
COPY --from=runtime-build --chown=app:app /app/src /app/src

USER 10001:10001
EXPOSE 8080

CMD ["health-data-pipeline-service"]
