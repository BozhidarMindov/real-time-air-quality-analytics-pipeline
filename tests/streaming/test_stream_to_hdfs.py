import importlib.util
import json
from pathlib import Path

import pytest
from kafka.errors import NoBrokersAvailable


def _load_module(module_name: str, module_path: Path, mocker):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_consumer_module(mocker):
    module_path = (
        Path(__file__).resolve().parents[2] / "src" / "streaming" / "consumer.py"
    )
    return _load_module("streaming_consumer_test_module", module_path, mocker)


def _load_hdfs_client_module(mocker):
    module_path = (
        Path(__file__).resolve().parents[2] / "src" / "streaming" / "hdfs_client.py"
    )
    return _load_module("streaming_hdfs_client_test_module", module_path, mocker)


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

    def ensure_directory(self, path):
        self.calls.append(("ensure_directory", path))

    def upload_file(self, local_path, remote_path):
        self.calls.append(("upload_file", Path(local_path), remote_path))


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


class FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeWebHDFSSession:
    def __init__(self):
        self.calls = []
        self.get_response = FakeResponse(status_code=200)
        self.put_responses = []
        self.post_responses = []

    def get(self, url, **kwargs):
        self.calls.append(("get", url, kwargs))
        return self.get_response

    def put(self, url, **kwargs):
        self.calls.append(("put", url, kwargs))
        return self.put_responses.pop(0)

    def post(self, url, **kwargs):
        self.calls.append(("post", url, kwargs))
        return self.post_responses.pop(0)


def test_consumer_extract_curated_record_keeps_only_required_fields(mocker):
    consumer_module = _load_consumer_module(mocker)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_hdfs_client",
        return_value=FakeHDFSClient(False),
    )
    consumer = consumer_module.Consumer()
    payload = {
        "data": {
            "idx": 42,
            "aqi": 64,
            "dominentpol": "pm10",
            "city": {"name": "Sofia", "geo": [42.6977, 23.3219]},
            "time": {"iso": "2026-04-07T10:00:00+03:00"},
            "iaqi": {
                "pm10": {"v": 31.5},
                "no2": {"v": 18.2},
                "o3": {"v": 11.4},
                "t": {"v": 19.1},
                "h": {"v": 47.0},
                "w": {"v": 3.5},
                "p": {"v": 1008.0},
                "dew": {"v": 7.2},
            },
        },
        "ignored": {"nested": "value"},
    }

    result = consumer.extract_curated_record(payload)

    assert result == {
        "timestamp": "2026-04-07T10:00:00+03:00",
        "station_id": 42,
        "station_name": "Sofia",
        "latitude": 42.6977,
        "longitude": 23.3219,
        "aqi": 64,
        "dominant_pollutant": "pm10",
        "pm10": 31.5,
        "no2": 18.2,
        "o3": 11.4,
        "temperature": 19.1,
        "humidity": 47.0,
        "wind": 3.5,
        "pressure": 1008.0,
        "dew": 7.2,
    }


def test_consumer_build_raw_output_path_uses_single_daily_jsonl_file(mocker):
    consumer_module = _load_consumer_module(mocker)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_hdfs_client",
        return_value=FakeHDFSClient(False),
    )
    consumer = consumer_module.Consumer(output_root="/data/air-quality", city="sofia")

    result = consumer.build_raw_output_path("2026-04-07")

    assert result == "/data/air-quality/sofia/raw/2026-04-07.jsonl"


def test_consumer_build_curated_output_path_uses_daily_jsonl_file(mocker):
    consumer_module = _load_consumer_module(mocker)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_hdfs_client",
        return_value=FakeHDFSClient(False),
    )
    consumer = consumer_module.Consumer(output_root="/data/air-quality", city="sofia")

    result = consumer.build_curated_output_path("2026-04-07")

    assert result == "/data/air-quality/sofia/curated/2026-04-07.jsonl"


def test_consumer_write_raw_records_appends_when_daily_file_exists(mocker):
    consumer_module = _load_consumer_module(mocker)
    fake_hdfs_client = FakeHDFSClient(exists_result=True)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )
    consumer = consumer_module.Consumer(output_root="/data/air-quality", city="sofia")
    records = [{"data": {"idx": 1}}, {"data": {"idx": 2}}]

    consumer.write_raw_records(records, "2026-04-07")

    assert fake_hdfs_client.calls == [
        ("exists", "/data/air-quality/sofia/raw/2026-04-07.jsonl"),
        (
            "append_text",
            "/data/air-quality/sofia/raw/2026-04-07.jsonl",
            '{"data":{"idx":1}}\n{"data":{"idx":2}}\n',
        ),
    ]


def test_consumer_write_curated_records_creates_daily_jsonl_when_missing(
    tmp_path, mocker
):
    consumer_module = _load_consumer_module(mocker)
    fake_hdfs_client = FakeHDFSClient(exists_result=False)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )
    consumer = consumer_module.Consumer(
        output_root="/data/air-quality",
        city="sofia",
        local_staging_dir=tmp_path,
    )
    curated_records = [
        {
            "timestamp": "2026-04-07T10:00:00+03:00",
            "station_id": 42,
            "station_name": "Sofia",
            "latitude": 42.6977,
            "longitude": 23.3219,
            "aqi": 64,
            "dominant_pollutant": "pm10",
            "pm10": 31.5,
            "no2": 18.2,
            "o3": 11.4,
            "temperature": 19.1,
            "humidity": 47.0,
            "wind": 3.5,
            "pressure": 1008.0,
            "dew": 7.2,
        }
    ]

    consumer.write_curated_records(curated_records, "2026-04-07")

    assert fake_hdfs_client.calls == [
        ("exists", "/data/air-quality/sofia/curated/2026-04-07.jsonl"),
        (
            "create_text",
            "/data/air-quality/sofia/curated/2026-04-07.jsonl",
            '{"timestamp":"2026-04-07T10:00:00+03:00","station_id":42,"station_name":"Sofia","latitude":42.6977,"longitude":23.3219,"aqi":64,"dominant_pollutant":"pm10","pm10":31.5,"no2":18.2,"o3":11.4,"temperature":19.1,"humidity":47.0,"wind":3.5,"pressure":1008.0,"dew":7.2}\n',
        ),
    ]


def test_consumer_write_curated_records_appends_to_daily_jsonl_when_present(
    tmp_path, mocker
):
    consumer_module = _load_consumer_module(mocker)
    fake_hdfs_client = FakeHDFSClient(exists_result=True)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )
    consumer = consumer_module.Consumer(
        output_root="/data/air-quality",
        city="sofia",
        local_staging_dir=tmp_path,
    )
    curated_records = [
        {
            "timestamp": "2026-04-07T10:00:00+03:00",
            "station_id": 42,
            "station_name": "Sofia",
            "latitude": 42.6977,
            "longitude": 23.3219,
            "aqi": 64,
            "dominant_pollutant": "pm10",
            "pm10": 31.5,
            "no2": 18.2,
            "o3": 11.4,
            "temperature": 19.1,
            "humidity": 47.0,
            "wind": 3.5,
            "pressure": 1008.0,
            "dew": 7.2,
        }
    ]

    consumer.write_curated_records(curated_records, "2026-04-07")

    assert fake_hdfs_client.calls == [
        ("exists", "/data/air-quality/sofia/curated/2026-04-07.jsonl"),
        (
            "append_text",
            "/data/air-quality/sofia/curated/2026-04-07.jsonl",
            '{"timestamp":"2026-04-07T10:00:00+03:00","station_id":42,"station_name":"Sofia","latitude":42.6977,"longitude":23.3219,"aqi":64,"dominant_pollutant":"pm10","pm10":31.5,"no2":18.2,"o3":11.4,"temperature":19.1,"humidity":47.0,"wind":3.5,"pressure":1008.0,"dew":7.2}\n',
        ),
    ]


def test_consumer_write_curated_records_skips_cached_and_batch_duplicates(
    tmp_path, mocker
):
    consumer_module = _load_consumer_module(mocker)
    cache_path = tmp_path / "curated_observation_cache.json"
    cache_path.write_text('{"42":"2026-04-07T10:00:00+03:00"}', encoding="utf-8")
    fake_hdfs_client = FakeHDFSClient(exists_result=True)
    logger = mocker.Mock()
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )
    consumer = consumer_module.Consumer(
        output_root="/data/air-quality",
        city="sofia",
        local_staging_dir=tmp_path,
        logger=logger,
    )
    curated_records = [
        {"timestamp": "2026-04-07T10:00:00+03:00", "station_id": 42},
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": 42},
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": 42},
    ]

    consumer.write_curated_records(curated_records, "2026-04-07")

    assert fake_hdfs_client.calls == [
        ("exists", "/data/air-quality/sofia/curated/2026-04-07.jsonl"),
        (
            "append_text",
            "/data/air-quality/sofia/curated/2026-04-07.jsonl",
            '{"timestamp":"2026-04-07T11:00:00+03:00","station_id":42}\n',
        ),
    ]
    assert cache_path.read_text(encoding="utf-8") == '{"42":"2026-04-07T11:00:00+03:00"}'
    logger.warning.assert_not_called()
    logger.info.assert_any_call(
        "Skipping duplicate curated record for station_id=42 timestamp=2026-04-07T10:00:00+03:00"
    )
    logger.info.assert_any_call(
        "Skipping duplicate curated record for station_id=42 timestamp=2026-04-07T11:00:00+03:00"
    )


def test_consumer_write_curated_records_skips_missing_dedup_fields(
    tmp_path, mocker
):
    consumer_module = _load_consumer_module(mocker)
    fake_hdfs_client = FakeHDFSClient(exists_result=False)
    logger = mocker.Mock()
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )
    consumer = consumer_module.Consumer(
        output_root="/data/air-quality",
        city="sofia",
        local_staging_dir=tmp_path,
        logger=logger,
    )
    curated_records = [
        {"timestamp": "2026-04-07T10:00:00+03:00", "station_id": 42},
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": None},
        {"timestamp": None, "station_id": 42},
    ]

    consumer.write_curated_records(curated_records, "2026-04-07")

    assert fake_hdfs_client.calls == [
        ("exists", "/data/air-quality/sofia/curated/2026-04-07.jsonl"),
        (
            "create_text",
            "/data/air-quality/sofia/curated/2026-04-07.jsonl",
            '{"timestamp":"2026-04-07T10:00:00+03:00","station_id":42}\n',
        ),
    ]
    assert (tmp_path / "curated_observation_cache.json").read_text(
        encoding="utf-8"
    ) == '{"42":"2026-04-07T10:00:00+03:00"}'
    assert logger.warning.call_count == 2


def test_consumer_raises_for_invalid_persisted_cache_file(tmp_path, mocker):
    consumer_module = _load_consumer_module(mocker)
    (tmp_path / "curated_observation_cache.json").write_text(
        "not-json", encoding="utf-8"
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=FakeHDFSClient(False)
    )
    with pytest.raises(json.JSONDecodeError):
        consumer_module.Consumer(
            output_root="/data/air-quality",
            city="sofia",
            local_staging_dir=tmp_path,
        )


def test_consumer_persists_cache_with_atomic_replace(tmp_path, mocker):
    consumer_module = _load_consumer_module(mocker)
    path_type = type(tmp_path)
    replace_spy = mocker.spy(path_type, "replace")
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_hdfs_client",
        return_value=FakeHDFSClient(False),
    )
    consumer = consumer_module.Consumer(
        output_root="/data/air-quality",
        city="sofia",
        local_staging_dir=tmp_path,
    )
    consumer.curated_observation_cache = {"42": "2026-04-07T10:00:00+03:00"}

    consumer._persist_curated_observation_cache()

    cache_path = tmp_path / "curated_observation_cache.json"
    temp_path = tmp_path / "curated_observation_cache.json.tmp"
    assert cache_path.read_text(encoding="utf-8") == '{"42":"2026-04-07T10:00:00+03:00"}'
    assert temp_path.exists() is False
    replace_spy.assert_called_once_with(temp_path, cache_path)


def test_consumer_consume_once_groups_messages_and_writes_outputs(tmp_path, mocker):
    consumer_module = _load_consumer_module(mocker)
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
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=fake_kafka_consumer,
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )
    consumer = consumer_module.Consumer(
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
            "curated_records": [],
        },
    }
    logger.info.assert_any_call("Wrote 1 messages for 2026-04-07 to HDFS")
    logger.info.assert_any_call("Wrote 1 messages for 2026-04-06 to HDFS")
    logger.warning.assert_called_once()


def test_consumer_commits_offsets_only_after_successful_hdfs_writes(tmp_path, mocker):
    consumer_module = _load_consumer_module(mocker)
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

    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=fake_kafka_consumer,
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )

    consumer = consumer_module.Consumer(
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


def test_consumer_group_messages_by_day_skips_invalid_json(mocker):
    consumer_module = _load_consumer_module(mocker)
    logger = mocker.Mock()
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_hdfs_client",
        return_value=FakeHDFSClient(False),
    )
    consumer = consumer_module.Consumer(processing_date="2026-04-07", logger=logger)

    grouped = consumer.group_messages_by_day([b"not-json", b'{"data":{"idx":7}}'])

    assert grouped == {
        "2026-04-07": {
            "raw_records": [{"data": {"idx": 7}}],
            "curated_records": [],
        }
    }
    assert logger.warning.call_count == 2


def test_consumer_run_processes_the_requested_number_of_iterations(mocker):
    consumer_module = _load_consumer_module(mocker)
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_kafka_consumer",
        return_value=FakeKafkaConsumer([]),
    )
    mocker.patch.object(
        consumer_module.Consumer,
        "_create_hdfs_client",
        return_value=FakeHDFSClient(False),
    )
    consumer = consumer_module.Consumer()
    consume_once = mocker.patch.object(consumer, "consume_once")

    consumer.run(iterations=3)

    assert consume_once.call_count == 3


def test_consumer_retries_kafka_connection_when_broker_is_temporarily_unavailable(
    mocker,
):
    consumer_module = _load_consumer_module(mocker)
    fake_hdfs_client = FakeHDFSClient(False)
    kafka_consumer = FakeKafkaConsumer([])
    sleep = mocker.Mock()

    mocker.patch.object(
        consumer_module,
        "KafkaConsumer",
        side_effect=[NoBrokersAvailable(), kafka_consumer],
    )
    mocker.patch.object(
        consumer_module.Consumer, "_create_hdfs_client", return_value=fake_hdfs_client
    )

    consumer = consumer_module.Consumer(
        kafka_connect_retry_attempts=2,
        kafka_connect_retry_backoff_seconds=7,
        sleep=sleep,
    )

    assert consumer.kafka_consumer is kafka_consumer
    assert consumer_module.KafkaConsumer.call_count == 2
    assert consumer_module.KafkaConsumer.call_args.kwargs["enable_auto_commit"] is False
    sleep.assert_called_once_with(7)


def test_hdfs_client_exists_returns_false_for_missing_path(mocker):
    hdfs_client_module = _load_hdfs_client_module(mocker)
    session = FakeWebHDFSSession()
    session.get_response = FakeResponse(status_code=404)
    client = hdfs_client_module.HDFSClient(
        namenode_url="http://namenode:9870",
        user="hdfs",
        session=session,
    )

    result = client.exists("/data/air-quality/sofia/raw/2026-04-07.jsonl")

    assert result is False
    assert session.calls == [
        (
            "get",
            "http://namenode:9870/webhdfs/v1/data/air-quality/sofia/raw/2026-04-07.jsonl",
            {
                "params": {"op": "GETFILESTATUS", "user.name": "hdfs"},
                "allow_redirects": False,
                "timeout": 30,
            },
        )
    ]


def test_hdfs_client_create_text_follows_namenode_redirect_to_datanode(mocker):
    hdfs_client_module = _load_hdfs_client_module(mocker)
    session = FakeWebHDFSSession()
    session.put_responses = [
        FakeResponse(
            status_code=307,
            headers={"Location": "http://datanode:9864/webhdfs/v1/data/file"},
        ),
        FakeResponse(status_code=201),
    ]
    client = hdfs_client_module.HDFSClient(
        namenode_url="http://namenode:9870",
        user="hdfs",
        session=session,
    )

    client.create_text("/data/file", "hello\n")

    assert session.calls == [
        (
            "put",
            "http://namenode:9870/webhdfs/v1/data/file",
            {
                "params": {"op": "CREATE", "user.name": "hdfs", "overwrite": "false"},
                "allow_redirects": False,
                "timeout": 30,
            },
        ),
        (
            "put",
            "http://datanode:9864/webhdfs/v1/data/file",
            {
                "data": b"hello\n",
                "timeout": 30,
            },
        ),
    ]


def test_hdfs_client_append_text_follows_namenode_redirect_to_datanode(mocker):
    hdfs_client_module = _load_hdfs_client_module(mocker)
    session = FakeWebHDFSSession()
    session.post_responses = [
        FakeResponse(
            status_code=307,
            headers={"Location": "http://datanode:9864/webhdfs/v1/data/file"},
        ),
        FakeResponse(status_code=200),
    ]
    client = hdfs_client_module.HDFSClient(
        namenode_url="http://namenode:9870",
        user="hdfs",
        session=session,
    )

    client.append_text("/data/file", "world\n")

    assert session.calls == [
        (
            "post",
            "http://namenode:9870/webhdfs/v1/data/file",
            {
                "params": {"op": "APPEND", "user.name": "hdfs"},
                "allow_redirects": False,
                "timeout": 30,
            },
        ),
        (
            "post",
            "http://datanode:9864/webhdfs/v1/data/file",
            {
                "data": b"world\n",
                "timeout": 30,
            },
        ),
    ]
