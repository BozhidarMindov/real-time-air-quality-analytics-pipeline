FROM python:3.11-slim

WORKDIR /app

ARG UV_VERSION=0.11.3

RUN pip install --no-cache-dir uv==${UV_VERSION}

ENV UV_NO_DEV=1

COPY pyproject.toml uv.lock README.md ./
COPY scripts/run_streaming.py ./scripts/run_streaming.py
COPY src/__init__.py ./src/__init__.py
COPY src/streaming ./src/streaming

RUN uv sync --locked

CMD ["uv", "run", "python", "scripts/run_streaming.py"]
