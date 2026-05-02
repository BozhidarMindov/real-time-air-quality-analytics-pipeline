FROM quay.io/jupyter/pyspark-notebook:2026-04-06

USER root

WORKDIR /app

ENV PYTHONPATH=/app

COPY src/__init__.py ./src/__init__.py
COPY src/common ./src/common
COPY src/analytics ./src/analytics
COPY notebooks ./notebooks
