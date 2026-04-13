# Real-Time Air Quality Analytics Pipeline

### A Dockerized data pipeline for collecting, curating, and analyzing Sofia air quality data

---

## Features

- Fetches live air quality data for `sofia` from the **[AQICN API](https://aqicn.org/api/)**
- Publishes raw observations to **Kafka** with a dedicated ingestion producer
- Consumes **Kafka** messages and stores both raw and curated daily JSONL datasets in **HDFS**
- Deduplicates curated station observations before persisting them
- Runs **Spark** batch analytics over the curated HDFS dataset
- Exposes a **Jupyter** notebook container for exploring analytics results
- Includes a **Kafka UI** and **HDFS NameNode UI** for an easier inspection of the pipeline
- Containerized setup with **Docker Compose**

---

## Tech Stack

[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)](https://jupyter.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

- **Data Source**: AQICN city feed API
- **Ingestion Layer**: Python producer + Kafka
- **Streaming Layer**: Python consumer with curation and deduplication
- **Storage Layer**: HDFS with raw and curated JSONL outputs
- **Analytics Layer**: PySpark batch analysis in Jupyter
- **Runtime/Orchestration**: Docker Compose

---

## How It Works

1. The `ingestion-producer` fetches Sofia air quality snapshots from the AQICN API.
2. Each source payload is published to the Kafka topic `air_quality_sofia`.
3. The `streaming-consumer` reads Kafka batches, groups records by day, and writes:
   - raw payloads to HDFS
   - curated, deduplicated observations to HDFS
4. The `analytics-notebook` container reads the curated HDFS dataset with Spark.
5. Batch analytics tables are generated for AQI trends, pollutant averages, pollutant frequency, and weather correlations.

### Pipeline Schema

```text
AQICN API
   |
   v
ingestion-producer
   |
   v
Kafka topic: air_quality_sofia
   |
   v
streaming-consumer
   |
   v
HDFS
  |- /data/air-quality/sofia/raw/*.jsonl
  |- /data/air-quality/sofia/curated/*.jsonl
   |
   v
analytics-notebook (Spark + Jupyter)
```

### Analytics Outputs

The batch analysis produces these report tables:

- `hourly_aqi`: average AQI grouped by hour of day
- `daily_aqi`: average AQI grouped by calendar day
- `aqi_category_distribution`: AQI counts grouped by AQI category
- `average_pollutants`: mean `pm10`, `no2`, and `o3` values
- `dominant_pollutants`: dominant pollutant counts ordered by frequency
- `weather_correlations`: AQI correlations with temperature, humidity, and wind

---

## Quick Start

### Requirements

- Docker
- Docker Compose
- AQICN API token

### Clone the Repo

```sh
git clone https://github.com/BozhidarMindov/real-time-air-quality-analytics-pipeline.git
```

### Environment Variables (`.env`)

Create a `.env` file in the project root:

```dotenv
AQICN_API_TOKEN=<your_token> # Required
CITY=sofia # Optional (default=sofia)
POLL_INTERVAL_SECONDS=60 # Optional
KAFKA_TOPIC=air_quality_sofia # Optional
```

Optional runtime overrides used by the pipeline:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
OUTPUT_ROOT=/data/air-quality
HDFS_NAMENODE_URL=http://namenode:9870
HDFS_USER=root
LOCAL_STAGING_DIR=/tmp/air-quality
```

### Docker Setup

1. Start the containers:

   ```sh
   docker compose up -d --build
   ```

2. When the containers are running, open the `real-time-air-quality-analytics-pipeline-analytics-notebook` container logs by executing:

   ```sh
   docker logs real-time-air-quality-analytics-pipeline-analytics-notebook
   ```

3. Look for the available Jupyter url logs, which should look something like this:

   ```txt
   [I 2026-04-12 11:25:26.074 ServerApp] http://localhost:8888/lab?token=7fecf7625414e7a57dd1e30db3fc846b56592cebf2ec387c
   [I 2026-04-12 11:25:26.074 ServerApp] http://127.0.0.1:8888/lab?token=7fecf7625414e7a57dd1e30db3fc846b56592cebf2ec387c
   ```

4. Click on either url to access the notebook (which is in the `notebooks` folder) and run the analytics.

### Useful URLs

- Kafka UI: `http://localhost:8080`
- HDFS NameNode UI: `http://localhost:9870`
- Jupyter Notebook: `http://localhost:8888`

---

## Notes

- The pipeline is currently configured around the `sofia` city feed by default.
- Raw and curated datasets are written to HDFS under `/data/air-quality/<city>/`.
- The analytics notebook container is intended for interactive Spark exploration on top of the curated dataset.
- This project was completed as part of the **Big Data Engineering course** in the **Big Data Technologies** master's program at **Sofia University**.
