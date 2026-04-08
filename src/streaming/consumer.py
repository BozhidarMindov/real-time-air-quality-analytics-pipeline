import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from src.streaming.hdfs_client import DEFAULT_HDFS_NAMENODE_URL
from src.streaming.hdfs_client import DEFAULT_HDFS_USER
from src.streaming.hdfs_client import HDFSClient
from src.streaming.hdfs_client import normalize_hdfs_path


DEFAULT_KAFKA_TOPIC = "air_quality_sofia"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9094"
DEFAULT_CITY = "sofia"
DEFAULT_OUTPUT_ROOT = "/data/air-quality"
DEFAULT_LOCAL_STAGING_DIR = "/tmp/air-quality"
DEFAULT_CONSUMER_GROUP = "air-quality-streaming"
DEFAULT_POLL_TIMEOUT_MS = 5000
DEFAULT_BATCH_SIZE = 100
DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS = 6
DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS = 5

CURATED_COLUMNS = (
    "timestamp",
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "aqi",
    "dominant_pollutant",
    "pm10",
    "no2",
    "o3",
    "temperature",
    "humidity",
    "wind",
    "pressure",
    "dew",
)


def default_processing_date() -> str:
    """Return the default processing date string.

    Returns:
        str: The current date in `YYYY-MM-DD` format.
    """

    return date.today().isoformat()


def join_hdfs_root(root: Path | str, *parts: str) -> str:
    """Join an HDFS root path with path parts.

    Args:
        root: An HDFS root path.
        *parts: Additional HDFS path parts.

    Returns:
        str: A normalized HDFS path.
    """

    base = normalize_hdfs_path(root)
    suffix = "/".join(part.strip("/") for part in parts if part)
    return f"{base}/{suffix}" if suffix else base


def get_nested(mapping: dict | None, *keys: str):
    """Return a nested dictionary value when present.

    Args:
        mapping: A dictionary-like mapping.
        *keys: A sequence of nested keys.

    Returns:
        Any: A nested value or `None`.
    """

    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


class Consumer:
    """A Kafka consumer that writes raw JSON and curated Parquet data to HDFS."""

    def __init__(
        self,
        aqicn_api_token: str = "",
        city: str = DEFAULT_CITY,
        kafka_bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        kafka_topic: str = DEFAULT_KAFKA_TOPIC,
        output_root: Path | str = DEFAULT_OUTPUT_ROOT,
        processing_date: str | None = None,
        hdfs_namenode_url: str = DEFAULT_HDFS_NAMENODE_URL,
        hdfs_user: str = DEFAULT_HDFS_USER,
        local_staging_dir: Path | str = DEFAULT_LOCAL_STAGING_DIR,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
        poll_timeout_ms: int = DEFAULT_POLL_TIMEOUT_MS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        kafka_connect_retry_attempts: int = DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS,
        kafka_connect_retry_backoff_seconds: int = DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS,
        logger: logging.Logger | None = None,
        sleep=time.sleep,
    ) -> None:
        """Initialize the streaming consumer.

        Args:
            aqicn_api_token: An unused placeholder kept for config symmetry with the producer.
            city: A city name used for output paths.
            kafka_bootstrap_servers: A comma-separated Kafka bootstrap server list.
            kafka_topic: A Kafka topic name.
            output_root: An HDFS root output path.
            processing_date: An optional fallback processing date.
            hdfs_namenode_url: A Namenode WebHDFS base URL.
            hdfs_user: An HDFS user name.
            local_staging_dir: A local directory used for temporary Parquet files.
            consumer_group: A Kafka consumer group id.
            poll_timeout_ms: A Kafka poll timeout in milliseconds.
            batch_size: A maximum number of Kafka messages per poll.
            kafka_connect_retry_attempts: A number of Kafka connection attempts before failing.
            kafka_connect_retry_backoff_seconds: A delay between Kafka connection attempts.
            logger: An optional application logger.
            sleep: A sleep function used between Kafka connection attempts.
        """

        self.aqicn_api_token = aqicn_api_token
        self.city = city
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.output_root = output_root
        self.processing_date = processing_date or default_processing_date()
        self.hdfs_namenode_url = hdfs_namenode_url
        self.hdfs_user = hdfs_user
        self.local_staging_dir = Path(local_staging_dir)
        self.consumer_group = consumer_group
        self.poll_timeout_ms = poll_timeout_ms
        self.batch_size = batch_size
        self.kafka_connect_retry_attempts = kafka_connect_retry_attempts
        self.kafka_connect_retry_backoff_seconds = kafka_connect_retry_backoff_seconds
        self.logger = logger or logging.getLogger("air_quality.streaming")
        self.sleep = sleep
        self.kafka_consumer = self._create_kafka_consumer()
        self.hdfs_client = self._create_hdfs_client()

    def run(self, iterations: int | None = None) -> None:
        """Run the streaming loop.

        Args:
            iterations: An optional number of polling iterations.
        """

        completed = 0
        try:
            while iterations is None or completed < iterations:
                self.consume_once()
                completed += 1
        finally:
            self.kafka_consumer.close()

    def consume_once(self) -> dict[str, dict[str, list[dict]]]:
        """Consume one Kafka batch and write it to HDFS.

        Returns:
            dict[str, dict[str, list[dict]]]: The grouped raw and curated records by day.
        """

        polled_records = self.kafka_consumer.poll(
            timeout_ms=self.poll_timeout_ms,
            max_records=self.batch_size,
        )
        messages = [record.value for records in polled_records.values() for record in records]
        if not messages:
            return {}

        grouped_records = self.group_messages_by_day(messages)
        for day, day_records in grouped_records.items():
            self.write_raw_records(day_records["raw_records"], day)
            self.write_curated_records(day_records["curated_records"], day)
            self.logger.info(f"Wrote {len(day_records['raw_records'])} messages for {day} to HDFS")

        return grouped_records

    def group_messages_by_day(self, messages: list[bytes | str]) -> dict[str, dict[str, list[dict]]]:
        """Group Kafka messages into raw and curated records by day.

        Args:
            messages: A list of Kafka message payloads.

        Returns:
            dict[str, dict[str, list[dict]]]: The grouped raw and curated records.
        """

        grouped_records: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: {"raw_records": [], "curated_records": []}
        )

        for message in messages:
            payload_text = message.decode("utf-8") if isinstance(message, bytes) else str(message)
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                self.logger.warning("Skipping invalid JSON message from Kafka")
                continue

            day = self.extract_day(payload)
            grouped_records[day]["raw_records"].append(payload)
            grouped_records[day]["curated_records"].append(self.extract_curated_record(payload))

        return dict(grouped_records)

    def extract_curated_record(self, payload: dict) -> dict:
        """Extract the curated AQICN fields used by the project.

        Args:
            payload: A raw AQICN payload.

        Returns:
            dict: A curated record with the required analytics fields.
        """

        data = payload.get("data") if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            data = {}

        return {
            "timestamp": get_nested(data, "time", "iso"),
            "station_id": data.get("idx"),
            "station_name": get_nested(data, "city", "name"),
            "latitude": self.get_geo_value(data, 0),
            "longitude": self.get_geo_value(data, 1),
            "aqi": data.get("aqi"),
            "dominant_pollutant": data.get("dominentpol"),
            "pm10": self.get_iaqi_value(data, "pm10"),
            "no2": self.get_iaqi_value(data, "no2"),
            "o3": self.get_iaqi_value(data, "o3"),
            "temperature": self.get_iaqi_value(data, "t"),
            "humidity": self.get_iaqi_value(data, "h"),
            "wind": self.get_iaqi_value(data, "w"),
            "pressure": self.get_iaqi_value(data, "p"),
            "dew": self.get_iaqi_value(data, "dew"),
        }

    def extract_day(self, payload: dict) -> str:
        """Extract the day partition from a raw payload.

        Args:
            payload: A raw AQICN payload.

        Returns:
            str: A day string in `YYYY-MM-DD` format.
        """

        timestamp = get_nested(payload, "data", "time", "iso")
        if isinstance(timestamp, str) and len(timestamp) >= 10:
            return timestamp[:10]
        return self.processing_date

    def build_raw_output_path(self, day: str) -> str:
        """Build the daily raw JSON output path.

        Args:
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            str: An HDFS path for the daily raw JSON file.
        """

        return join_hdfs_root(self.output_root, self.city, "raw", f"{day}.jsonl")

    def build_curated_output_path(self, day: str) -> str:
        """Build the daily curated Parquet output directory.

        Args:
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            str: An HDFS directory for the daily curated dataset.
        """

        return join_hdfs_root(self.output_root, self.city, "curated", f"day={day}")

    def write_raw_records(self, records: list[dict], day: str) -> str | None:
        """Write raw JSON records to the daily HDFS JSON Lines file.

        Args:
            records: A list of raw payloads.
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            str | None: An HDFS file path when records were written.
        """

        if not records:
            return None

        path = self.build_raw_output_path(day)
        content = "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
        if self.hdfs_client.exists(path):
            self.hdfs_client.append_text(path, content)
        else:
            self.hdfs_client.create_text(path, content)

        return path

    def write_curated_records(self, curated_records: list[dict], day: str) -> str | None:
        """Write curated records into the daily HDFS Parquet dataset.

        Args:
            curated_records: A list of curated records.
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            str | None: An HDFS file path when records were written.
        """

        if not curated_records:
            return None

        remote_directory = self.build_curated_output_path(day)
        part_name = self._build_parquet_part_name()
        local_path = self.local_staging_dir / part_name
        remote_path = f"{remote_directory}/{part_name}"

        self.hdfs_client.ensure_directory(remote_directory)
        self._write_parquet_file(curated_records, local_path)
        try:
            self.hdfs_client.upload_file(local_path, remote_path)
        finally:
            if local_path.exists():
                local_path.unlink()

        return remote_path

    def get_geo_value(self, data: dict, index: int):
        """Return a latitude or longitude value from the AQICN geo array.

        Args:
            data: A raw AQICN data payload.
            index: A zero-based geo array index.

        Returns:
            Any: A geo value or `None`.
        """

        geo = get_nested(data, "city", "geo")
        if isinstance(geo, list) and len(geo) > index:
            return geo[index]
        return None

    def get_iaqi_value(self, data: dict, key: str):
        """Return a pollutant or weather value from `data.iaqi`.

        Args:
            data: A raw AQICN data payload.
            key: An `iaqi` field name.

        Returns:
            Any: A nested `iaqi` value or `None`.
        """

        return get_nested(data, "iaqi", key, "v")

    def _build_parquet_part_name(self, now: datetime | None = None) -> str:
        """Build a unique Parquet part file name.

        Args:
            now: An optional timestamp override.

        Returns:
            str: A Parquet part file name.
        """

        stamp = (now or datetime.utcnow()).strftime("%Y%m%d%H%M%S%f")
        return f"part-{stamp}.parquet"

    def _write_parquet_file(self, records: list[dict], destination: Path) -> None:
        """Write curated records into a local Parquet file.

        Args:
            records: A list of curated records.
            destination: A local file path.
        """

        destination.parent.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame.from_records(records, columns=CURATED_COLUMNS)
        frame.to_parquet(destination, index=False)

    def _create_kafka_consumer(self):
        """Create the Kafka consumer used by the streaming consumer.

        Returns:
            KafkaConsumer: A configured Kafka consumer instance.
        """

        bootstrap_servers = [
            server.strip()
            for server in self.kafka_bootstrap_servers.split(",")
            if server.strip()
        ]
        last_error = None
        for attempt in range(1, self.kafka_connect_retry_attempts + 1):
            try:
                return KafkaConsumer(
                    self.kafka_topic,
                    bootstrap_servers=bootstrap_servers,
                    group_id=self.consumer_group,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda value: value,
                )
            except NoBrokersAvailable as exc:
                last_error = exc
                if attempt >= self.kafka_connect_retry_attempts:
                    break
                self.logger.warning(
                    f"Kafka broker not available on attempt "
                    f"{attempt}/{self.kafka_connect_retry_attempts}; "
                    f"retrying in {self.kafka_connect_retry_backoff_seconds} seconds"
                )
                self.sleep(self.kafka_connect_retry_backoff_seconds)

        assert last_error is not None
        raise last_error

    def _create_hdfs_client(self) -> HDFSClient:
        """Create the HDFS client used by the streaming consumer.

        Returns:
            HDFSClient: A configured HDFS client instance.
        """

        return HDFSClient(
            namenode_url=self.hdfs_namenode_url,
            user=self.hdfs_user,
        )
