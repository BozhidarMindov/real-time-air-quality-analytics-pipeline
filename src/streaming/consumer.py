import json
import logging
import tempfile
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

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
DEFAULT_LOCAL_STAGING_DIR = str(Path(tempfile.gettempdir()) / "air-quality")
DEFAULT_CURATED_CACHE_FILE_NAME = "curated_observation_cache.json"
DEFAULT_CONSUMER_GROUP = "air-quality-streaming"
DEFAULT_POLL_TIMEOUT_MS = 5000
DEFAULT_BATCH_SIZE = 100
DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS = 6
DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS = 5


def default_processing_date() -> str:
    """Return the default processing date string.

    Returns:
        The current date in `YYYY-MM-DD` format.
    """
    return date.today().isoformat()


def join_hdfs_root(root: Path | str, *parts: str) -> str:
    """Join an HDFS root path with path parts.

    Args:
        root: An HDFS root path.
        *parts: Additional HDFS path parts.

    Returns:
        The normalized HDFS path.
    """
    base = normalize_hdfs_path(root)
    suffix = "/".join(part.strip("/") for part in parts if part)
    return f"{base}/{suffix}" if suffix else base


def get_nested(mapping: dict | None, *keys: str):
    """Return a nested value when present.

    Args:
        mapping: A mapping that may contain nested values.
        *keys: A sequence of nested keys.

    Returns:
        The nested value when every key exists, or `None` otherwise.
    """
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


class Consumer:
    """A Kafka consumer that writes raw and curated JSON data to HDFS.

    Attributes:
        city: The city name used in HDFS output paths.
        kafka_bootstrap_servers: The configured Kafka bootstrap server list.
        kafka_topic: The Kafka topic consumed by the streaming job.
        output_root: The root HDFS path used for daily output files.
        processing_date: The fallback day used when a payload timestamp is missing.
        hdfs_namenode_url: The Namenode WebHDFS base URL.
        hdfs_user: The HDFS user name sent with WebHDFS requests.
        local_staging_dir: The local staging path used for the curated dedup cache file.
        consumer_group: The Kafka consumer group id.
        poll_timeout_ms: The Kafka poll timeout in milliseconds.
        batch_size: The maximum number of messages requested per poll.
        kafka_connect_retry_attempts: The maximum number of Kafka connection attempts.
        kafka_connect_retry_backoff_seconds: The delay between Kafka connection attempts.
        logger: The application logger for streaming events.
        sleep: The sleep function used between Kafka connection attempts.
        kafka_consumer: The Kafka consumer used to read source messages.
        hdfs_client: The WebHDFS client used to persist raw and curated records.
        curated_observation_cache_path: The local JSON cache file for the last seen observation per station.
        curated_observation_cache: The persisted dedup cache keyed by station id.
    """

    def __init__(
        self,
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
            city: A city name used for output paths.
            kafka_bootstrap_servers: A comma-separated Kafka bootstrap server list.
            kafka_topic: A Kafka topic name.
            output_root: An HDFS root output path.
            processing_date: An optional fallback processing date.
            hdfs_namenode_url: A Namenode WebHDFS base URL.
            hdfs_user: An HDFS user name.
            local_staging_dir: The local staging directory used for the curated dedup cache.
            consumer_group: A Kafka consumer group id.
            poll_timeout_ms: A Kafka poll timeout in milliseconds.
            batch_size: A maximum number of Kafka messages per poll.
            kafka_connect_retry_attempts: A number of Kafka connection attempts before failing.
            kafka_connect_retry_backoff_seconds: A delay between Kafka connection attempts.
            logger: An optional application logger.
            sleep: A sleep function used between Kafka connection attempts.
        """
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
        self.curated_observation_cache_path = (
            self.local_staging_dir / DEFAULT_CURATED_CACHE_FILE_NAME
        )
        self.curated_observation_cache = (
            json.loads(
                self.curated_observation_cache_path.read_text(encoding="utf-8")
            )
            if self.curated_observation_cache_path.exists()
            else {}
        )

    def run(self, iterations: int | None = None) -> None:
        """Run the streaming loop.

        Args:
            iterations: An optional number of polling iterations.

        Returns:
            None.
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
            The grouped raw and curated records for each processed day.
        """
        polled_records = self.kafka_consumer.poll(
            timeout_ms=self.poll_timeout_ms,
            max_records=self.batch_size,
        )
        messages = [
            record.value for records in polled_records.values() for record in records
        ]
        if not messages:
            return {}

        grouped_records = self.group_messages_by_day(messages)
        for day, day_records in grouped_records.items():
            self.write_raw_records(day_records["raw_records"], day)
            self.write_curated_records(day_records["curated_records"], day)
            self.logger.info(
                f"Wrote {len(day_records['raw_records'])} messages for {day} to HDFS"
            )

        self.kafka_consumer.commit()
        return grouped_records

    def group_messages_by_day(
        self, messages: list[bytes | str]
    ) -> dict[str, dict[str, list[dict]]]:
        """Group Kafka messages into raw and curated records by day.

        Args:
            messages: The Kafka message payloads to group by day.

        Returns:
            The grouped raw and curated records keyed by day.
        """
        grouped_records: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: {"raw_records": [], "curated_records": []}
        )

        for message in messages:
            payload_text = (
                message.decode("utf-8") if isinstance(message, bytes) else str(message)
            )
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                self.logger.warning("Skipping invalid JSON message from Kafka")
                continue

            day = self.extract_day(payload)
            curated_record = self.extract_curated_record(payload)
            grouped_records[day]["raw_records"].append(payload)
            if self.build_curated_record_key(curated_record) is not None:
                grouped_records[day]["curated_records"].append(curated_record)

        return dict(grouped_records)

    def extract_curated_record(self, payload: dict) -> dict:
        """Extract the curated AQICN fields used by the project.

        Args:
            payload: A raw AQICN payload.

        Returns:
            The curated record with the required analytics fields.
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
            The day string in `YYYY-MM-DD` format.
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
            The HDFS path for the daily raw JSON file.
        """
        return join_hdfs_root(self.output_root, self.city, "raw", f"{day}.jsonl")

    def build_curated_output_path(self, day: str) -> str:
        """Build the daily curated JSON output path.

        Args:
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            The HDFS path for the daily curated JSON Lines file.
        """
        return join_hdfs_root(self.output_root, self.city, "curated", f"{day}.jsonl")

    def write_raw_records(self, records: list[dict], day: str) -> str | None:
        """Write raw JSON records to the daily HDFS JSON Lines file.

        Args:
            records: The raw payloads to append for the requested day.
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            The written HDFS file path, or `None` when there is nothing to write.
        """
        if not records:
            return None

        path = self.build_raw_output_path(day)
        content = "".join(
            json.dumps(record, separators=(",", ":")) + "\n" for record in records
        )
        if self.hdfs_client.exists(path):
            self.hdfs_client.append_text(path, content)
        else:
            self.hdfs_client.create_text(path, content)

        return path

    def write_curated_records(
        self, curated_records: list[dict], day: str
    ) -> str | None:
        """Write curated records to the daily HDFS JSON Lines file.

        Args:
            curated_records: The curated records to append for the requested day.
            day: A day string in `YYYY-MM-DD` format.

        Returns:
            The written HDFS file path, or `None` when there is nothing to write.
        """
        if not curated_records:
            return None

        path = self.build_curated_output_path(day)
        filtered_records, updated_cache = self.filter_curated_records(curated_records)
        if not filtered_records:
            return None

        content = "".join(
            json.dumps(record, separators=(",", ":")) + "\n"
            for record in filtered_records
        )
        if self.hdfs_client.exists(path):
            self.hdfs_client.append_text(path, content)
        else:
            self.hdfs_client.create_text(path, content)

        self.curated_observation_cache = updated_cache
        self._persist_curated_observation_cache()
        return path

    def build_curated_record_key(self, curated_record: dict) -> tuple[str, str] | None:
        """Return the deduplication key for a curated record.

        Args:
            curated_record: The curated AQICN record.

        Returns:
            The `(station_id, timestamp)` key, or `None` when either field is missing.
        """
        station_id = curated_record.get("station_id")
        timestamp = curated_record.get("timestamp")
        if station_id is None or timestamp is None:
            self.logger.warning(
                "Skipping curated record without station_id or timestamp"
            )
            return None

        return str(station_id), timestamp

    def filter_curated_records(
        self, curated_records: list[dict]
    ) -> tuple[list[dict], dict[str, str]]:
        """Filter curated records against the persisted observation cache.

        Args:
            curated_records: The curated records for the current day.

        Returns:
            The curated records to write and the updated cache state.
        """
        updated_cache = dict(self.curated_observation_cache)
        filtered_records: list[dict] = []

        for record in curated_records:
            key = self.build_curated_record_key(record)
            if key is None:
                continue

            station_id, timestamp = key
            last_seen_timestamp = updated_cache.get(station_id)
            if last_seen_timestamp == timestamp:
                self.logger.info(
                    f"Skipping duplicate curated record for station_id={station_id} timestamp={timestamp}"
                )
                continue

            filtered_records.append(record)
            updated_cache[station_id] = timestamp

        return filtered_records, updated_cache

    def _persist_curated_observation_cache(self) -> None:
        """Persist the curated observation cache to local storage.

        Returns:
            None.
        """
        self.local_staging_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.curated_observation_cache_path.with_suffix(
            f"{self.curated_observation_cache_path.suffix}.tmp"
        )
        temp_path.write_text(
            json.dumps(
                self.curated_observation_cache,
                separators=(",", ":"),
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        temp_path.replace(self.curated_observation_cache_path)

    def get_geo_value(self, data: dict, index: int):
        """Return a latitude or longitude value from the AQICN geo array.

        Args:
            data: A raw AQICN data payload.
            index: A zero-based geo array index.

        Returns:
            The geo value at the requested position, or `None` when it is missing.
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
            The nested `iaqi` value, or `None` when it is missing.
        """
        return get_nested(data, "iaqi", key, "v")

    def _create_kafka_consumer(self):
        """Create the Kafka consumer used by the streaming consumer.

        Returns:
            The reader used for streaming messages.

        Raises:
            NoBrokersAvailable: A connection error after all Kafka retry attempts are exhausted.
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
                    enable_auto_commit=False,
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
            The writer used for HDFS persistence.
        """
        return HDFSClient(
            namenode_url=self.hdfs_namenode_url,
            user=self.hdfs_user,
        )
