FROM python:3.11-slim

WORKDIR /app

ARG UV_VERSION=0.11.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv==${UV_VERSION}

ENV UV_NO_DEV=1

COPY pyproject.toml uv.lock README.md ./
COPY scripts/run_analytics.py ./scripts/run_analytics.py
COPY src ./src
COPY notebooks ./notebooks

RUN uv sync --locked

CMD ["uv", "run", "python", "scripts/run_analytics.py"]
