FROM python:3.11-slim

WORKDIR /app

ARG UV_VERSION=0.11.3

RUN pip install --no-cache-dir uv==${UV_VERSION}

ENV UV_NO_DEV=1

COPY src/ingestion/pyproject.toml ./pyproject.toml
COPY src/ingestion/uv.lock ./uv.lock
COPY scripts/run_producer.py ./scripts/run_producer.py
COPY src/__init__.py ./src/__init__.py
COPY src/common ./src/common
COPY src/ingestion ./src/ingestion

RUN uv sync --locked

CMD ["uv", "run", "python", "scripts/run_producer.py"]
