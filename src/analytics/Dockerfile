FROM quay.io/jupyter/pyspark-notebook

USER root

WORKDIR /app

ENV PYTHONPATH=/app

COPY scripts/run_analytics.py ./scripts/run_analytics.py
COPY src/__init__.py ./src/__init__.py
COPY src/common ./src/common
COPY src/analytics ./src/analytics
COPY notebooks ./notebooks
