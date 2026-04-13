import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from src.streaming.hdfs_client import HDFSClient
from src.streaming.curation import extract_curated_record
from src.streaming.curation import extract_day
from src.streaming.curation import filter_curated_records
from src.streaming.curation import load_curated_observation_cache
from src.streaming.curation import persist_curated_observation_cache
from src.streaming.utils import build_daily_output_path
from src.streaming.utils import serialize_jsonl

from kafka import KafkaConsumer

DEFAULT_CURATED_CACHE_FILE_NAME = "curated_observation_cache.json"
DEFAULT_POLL_TIMEOUT_MS = 5000
DEFAULT_BATCH_SIZE = 100


class Consumer:
    """Consume Kafka messages and write raw and curated JSON data to HDFS.

    Attributes:
        kafka_consumer: The Kafka consumer used to read source messages.
        hdfs_client: The HDFS client used to persist raw and curated records.
        city: The city name used in output paths.
        output_root: The HDFS root path used for the daily output files.
        processing_date: The fallback day used when a payload timestamp is missing.
        local_staging_dir: The local directory used for the curated dedup cache file.
        poll_timeout_ms: The Kafka poll timeout in milliseconds.
        batch_size: The maximum number of messages requested per poll.
        logger: The application logger for streaming events.
        curated_observation_cache_path: The local JSON cache file for the last seen observation per station.
        curated_observation_cache: The persisted dedup cache keyed by station id.
    """
    def __init__(
        self,
        kafka_consumer: KafkaConsumer,
        hdfs_client: HDFSClient,
        city: str,
        output_root: Path | str,
        local_staging_dir: Path | str,
        processing_date: str | None = None,
        poll_timeout_ms: int = DEFAULT_POLL_TIMEOUT_MS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the streaming consumer.

        Args:
            kafka_consumer: A Kafka consumer instance that yields raw source messages.
            hdfs_client: An HDFS client instance used for raw and curated writes.
            city: A city name used in the storage layout.
            output_root: An HDFS root output path.
            processing_date: An optional fallback processing date.
            local_staging_dir: The local staging directory used for the curated dedup cache.
            poll_timeout_ms: A Kafka poll timeout in milliseconds.
            batch_size: A maximum number of Kafka messages per poll.
            logger: An optional application logger.
        """
        self.kafka_consumer = kafka_consumer
        self.hdfs_client = hdfs_client
        self.city = city
        self.output_root = output_root
        self.processing_date = processing_date or date.today().isoformat()
        self.local_staging_dir = Path(local_staging_dir)
        self.poll_timeout_ms = poll_timeout_ms
        self.batch_size = batch_size
        self.logger = logger or logging.getLogger("air_quality.streaming")
        self.curated_observation_cache_path = (
            self.local_staging_dir / DEFAULT_CURATED_CACHE_FILE_NAME
        )
        self.curated_observation_cache = load_curated_observation_cache(
            self.curated_observation_cache_path
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
            self._write_day_records(day, day_records)
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

            day = extract_day(payload, self.processing_date)
            curated_record = extract_curated_record(payload)
            grouped_records[day]["raw_records"].append(payload)
            grouped_records[day]["curated_records"].append(curated_record)

        return dict(grouped_records)

    def _write_records(self, path: str, records: list[dict]) -> None:
        """Write a JSONL payload to HDFS, creating or appending as needed.

        Args:
            path: An HDFS file path.
            records: The records to serialize and write.

        Returns:
            None.
        """
        if not records:
            return

        content = serialize_jsonl(records)
        if self.hdfs_client.exists(path):
            self.hdfs_client.append_text(path, content)
        else:
            self.hdfs_client.create_text(path, content)

    def _write_day_records(self, day: str, day_records: dict[str, list[dict]]) -> None:
        """Write one day's raw and curated records to HDFS.

        Args:
            day: A day string in `YYYY-MM-DD` format.
            day_records: The raw and curated records grouped for the requested day.

        Returns:
            None.
        """
        self._write_records(
            build_daily_output_path(self.output_root, self.city, "raw", day),
            day_records["raw_records"],
        )

        filtered_curated_records, updated_cache = filter_curated_records(
            day_records["curated_records"],
            self.curated_observation_cache,
            logger=self.logger,
        )
        if filtered_curated_records:
            self._write_records(
                build_daily_output_path(self.output_root, self.city, "curated", day),
                filtered_curated_records,
            )
            self.curated_observation_cache = updated_cache
            persist_curated_observation_cache(
                self.curated_observation_cache_path,
                self.curated_observation_cache,
            )
