import json

from src.streaming.consumer import Consumer


class FakeHDFSClient:
    def __init__(self, exists_result: bool):
        self.exists_result = exists_result
        self.calls = []

    def exists(self, path):
        self.calls.append(("exists", path))
        return self.exists_result

    def create_text(self, path, content):
        self.calls.append(("create_text", path, content))

    def append_text(self, path, content):
        self.calls.append(("append_text", path, content))


class FakeKafkaRecord:
    def __init__(self, value):
        self.value = value


class FakeKafkaConsumer:
    def __init__(self, polled_records):
        self.polled_records = list(polled_records)
        self.poll_calls = []
        self.closed = False
        self.commit_count = 0

    def poll(self, timeout_ms, max_records):
        self.poll_calls.append((timeout_ms, max_records))
        if self.polled_records:
            return self.polled_records.pop(0)
        return {}

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


def test_consumer_consume_once_groups_messages_and_writes_outputs(tmp_path, mocker):
    polled_records = [
        {
            "partition-0": [
                FakeKafkaRecord(
                    json.dumps(
                        {
                            "data": {
                                "time": {"iso": "2026-04-07T10:00:00+03:00"},
                                "idx": 1,
                            }
                        }
                    ).encode("utf-8")
                ),
                FakeKafkaRecord(json.dumps({"data": {"idx": 2}}).encode("utf-8")),
            ]
        }
    ]
    fake_kafka_consumer = FakeKafkaConsumer(polled_records=polled_records)
    fake_hdfs_client = FakeHDFSClient(exists_result=False)
    logger = mocker.Mock()
    consumer = Consumer(
        kafka_consumer=fake_kafka_consumer,
        hdfs_client=fake_hdfs_client,
        output_root="/data/air-quality",
        city="sofia",
        processing_date="2026-04-06",
        local_staging_dir=tmp_path,
        logger=logger,
    )

    result = consumer.consume_once()

    assert result == {
        "2026-04-07": {
            "raw_records": [
                {"data": {"time": {"iso": "2026-04-07T10:00:00+03:00"}, "idx": 1}}
            ],
            "curated_records": [
                {
                    "timestamp": "2026-04-07T10:00:00+03:00",
                    "station_id": 1,
                    "station_name": None,
                    "latitude": None,
                    "longitude": None,
                    "aqi": None,
                    "dominant_pollutant": None,
                    "pm10": None,
                    "no2": None,
                    "o3": None,
                    "temperature": None,
                    "humidity": None,
                    "wind": None,
                    "pressure": None,
                    "dew": None,
                }
            ],
        },
        "2026-04-06": {
            "raw_records": [{"data": {"idx": 2}}],
            "curated_records": [
                {
                    "timestamp": None,
                    "station_id": 2,
                    "station_name": None,
                    "latitude": None,
                    "longitude": None,
                    "aqi": None,
                    "dominant_pollutant": None,
                    "pm10": None,
                    "no2": None,
                    "o3": None,
                    "temperature": None,
                    "humidity": None,
                    "wind": None,
                    "pressure": None,
                    "dew": None,
                }
            ],
        },
    }
    logger.info.assert_any_call("Wrote 1 messages for 2026-04-07 to HDFS")
    logger.info.assert_any_call("Wrote 1 messages for 2026-04-06 to HDFS")
    logger.warning.assert_called_once()


def test_consumer_commits_offsets_only_after_successful_hdfs_writes(tmp_path):
    polled_records = [
        {
            "partition-0": [
                FakeKafkaRecord(
                    json.dumps(
                        {
                            "data": {
                                "time": {"iso": "2026-04-07T10:00:00+03:00"},
                                "idx": 1,
                            }
                        }
                    ).encode("utf-8")
                )
            ]
        }
    ]
    fake_kafka_consumer = FakeKafkaConsumer(polled_records=polled_records)
    fake_hdfs_client = FakeHDFSClient(exists_result=False)
    consumer = Consumer(
        kafka_consumer=fake_kafka_consumer,
        hdfs_client=fake_hdfs_client,
        output_root="/data/air-quality",
        city="sofia",
        processing_date="2026-04-06",
        local_staging_dir=tmp_path,
    )

    consumer.consume_once()

    assert fake_hdfs_client.calls == [
        ("exists", "/data/air-quality/sofia/raw/2026-04-07.jsonl"),
        (
            "create_text",
            "/data/air-quality/sofia/raw/2026-04-07.jsonl",
            '{"data":{"time":{"iso":"2026-04-07T10:00:00+03:00"},"idx":1}}\n',
        ),
        ("exists", "/data/air-quality/sofia/curated/2026-04-07.jsonl"),
        (
            "create_text",
            "/data/air-quality/sofia/curated/2026-04-07.jsonl",
            '{"timestamp":"2026-04-07T10:00:00+03:00","station_id":1,"station_name":null,"latitude":null,"longitude":null,"aqi":null,"dominant_pollutant":null,"pm10":null,"no2":null,"o3":null,"temperature":null,"humidity":null,"wind":null,"pressure":null,"dew":null}\n',
        ),
    ]
    assert fake_kafka_consumer.commit_count == 1


def test_consumer_group_messages_by_day_keeps_curated_projection_when_dedup_fields_are_missing(
    mocker,
):
    logger = mocker.Mock()
    consumer = Consumer(
        kafka_consumer=FakeKafkaConsumer([]),
        hdfs_client=FakeHDFSClient(False),
        city="sofia",
        output_root="/data/air-quality",
        local_staging_dir=".",
        processing_date="2026-04-07",
        logger=logger,
    )

    grouped = consumer.group_messages_by_day([b"not-json", b'{"data":{"idx":7}}'])

    assert grouped == {
        "2026-04-07": {
            "raw_records": [{"data": {"idx": 7}}],
            "curated_records": [
                {
                    "timestamp": None,
                    "station_id": 7,
                    "station_name": None,
                    "latitude": None,
                    "longitude": None,
                    "aqi": None,
                    "dominant_pollutant": None,
                    "pm10": None,
                    "no2": None,
                    "o3": None,
                    "temperature": None,
                    "humidity": None,
                    "wind": None,
                    "pressure": None,
                    "dew": None,
                }
            ],
        }
    }
    assert logger.warning.call_count == 1


def test_consumer_run_processes_the_requested_number_of_iterations(mocker):
    consumer = Consumer(
        kafka_consumer=FakeKafkaConsumer([]),
        hdfs_client=FakeHDFSClient(False),
        city="sofia",
        output_root="/data/air-quality",
        local_staging_dir=".",
    )
    consume_once = mocker.patch.object(consumer, "consume_once")

    consumer.run(iterations=3)

    assert consume_once.call_count == 3
