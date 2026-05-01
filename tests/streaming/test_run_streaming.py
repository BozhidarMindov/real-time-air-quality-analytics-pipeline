import importlib.util
from pathlib import Path

import pytest
from kafka.errors import NoBrokersAvailable


def _load_run_streaming_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "run_streaming.py"
    spec = importlib.util.spec_from_file_location(
        "run_streaming_test_module", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_main_reads_environment_and_forwards_values(mocker):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "KAFKA_TOPIC": "ignored_topic",
        "CITY": "varna",
        "OUTPUT_ROOT": "/warehouse/air-quality",
        "PROCESSING_DATE": "2026-04-06",
        "HDFS_NAMENODE_URL": "http://namenode:9870",
        "HDFS_USER": "airflow",
        "LOCAL_STAGING_DIR": "/tmp/air-quality",
    }
    consumer_instance = mocker.Mock()
    kafka_consumer = mocker.Mock()
    hdfs_client = mocker.Mock()
    consumer_class = mocker.patch.object(
        run_streaming, "Consumer", return_value=consumer_instance
    )
    kafka_consumer_class = mocker.patch.object(
        run_streaming, "KafkaConsumer", return_value=kafka_consumer
    )
    hdfs_client_class = mocker.patch.object(
        run_streaming, "HDFSClient", return_value=hdfs_client
    )
    configure_logging = mocker.patch.object(run_streaming, "configure_logging")
    mocker.patch.object(run_streaming.logging, "getLogger", return_value=mocker.Mock())
    getenv = mocker.patch.object(
        run_streaming.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_streaming.main()

    assert result == 0
    configure_logging.assert_called_once_with()
    assert getenv.call_count == 7
    kafka_consumer_class.assert_called_once_with(
        "air_quality_varna",
        bootstrap_servers=["broker:9092"],
        group_id=run_streaming.DEFAULT_CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=run_streaming.IDENTITY_DESERIALIZER,
    )
    hdfs_client_class.assert_called_once_with(
        namenode_url="http://namenode:9870",
        user="airflow",
    )
    consumer_class.assert_called_once_with(
        kafka_consumer=kafka_consumer,
        hdfs_client=hdfs_client,
        city="varna",
        output_root="/warehouse/air-quality",
        processing_date="2026-04-06",
        local_staging_dir="/tmp/air-quality",
    )
    consumer_instance.run.assert_called_once_with()


def test_main_uses_non_required_streaming_defaults_when_optional_env_is_missing_or_blank(
    mocker,
):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": "",
        "KAFKA_TOPIC": None,
        "CITY": "sofia",
        "OUTPUT_ROOT": "",
        "PROCESSING_DATE": "",
        "HDFS_NAMENODE_URL": "",
        "HDFS_USER": None,
        "LOCAL_STAGING_DIR": "",
    }
    consumer_instance = mocker.Mock()
    kafka_consumer = mocker.Mock()
    hdfs_client = mocker.Mock()
    consumer_class = mocker.patch.object(
        run_streaming, "Consumer", return_value=consumer_instance
    )
    kafka_consumer_class = mocker.patch.object(
        run_streaming, "KafkaConsumer", return_value=kafka_consumer
    )
    hdfs_client_class = mocker.patch.object(
        run_streaming, "HDFSClient", return_value=hdfs_client
    )
    mocker.patch.object(run_streaming, "configure_logging")
    mocker.patch.object(run_streaming.logging, "getLogger", return_value=mocker.Mock())
    mocker.patch.object(
        run_streaming.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_streaming.main()

    assert result == 0
    kafka_consumer_class.assert_called_once_with(
        "air_quality_sofia",
        bootstrap_servers=["localhost:9094"],
        group_id=run_streaming.DEFAULT_CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=run_streaming.IDENTITY_DESERIALIZER,
    )
    hdfs_client_class.assert_called_once_with(
        namenode_url=run_streaming.DEFAULT_HDFS_NAMENODE_URL,
        user=run_streaming.DEFAULT_HDFS_USER,
    )
    consumer_class.assert_called_once_with(
        kafka_consumer=kafka_consumer,
        hdfs_client=hdfs_client,
        city="sofia",
        output_root="/data/air-quality",
        processing_date=None,
        local_staging_dir=run_streaming.DEFAULT_LOCAL_STAGING_DIR,
    )
    consumer_instance.run.assert_called_once_with()


@pytest.mark.parametrize("missing_value", [None, ""])
def test_main_raises_when_city_is_missing(mocker, missing_value):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": None,
        "KAFKA_TOPIC": None,
        "CITY": missing_value,
        "OUTPUT_ROOT": None,
        "PROCESSING_DATE": None,
        "HDFS_NAMENODE_URL": None,
        "HDFS_USER": None,
        "LOCAL_STAGING_DIR": None,
    }
    mocker.patch.object(run_streaming, "configure_logging")
    mocker.patch.object(run_streaming.logging, "getLogger", return_value=mocker.Mock())
    mocker.patch.object(
        run_streaming.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    with pytest.raises(ValueError, match="CITY is required"):
        run_streaming.main()


def test_main_builds_kafka_topic_from_city_when_topic_env_is_missing(mocker):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",
        "KAFKA_TOPIC": "",
        "CITY": "varna",
        "OUTPUT_ROOT": None,
        "PROCESSING_DATE": None,
        "HDFS_NAMENODE_URL": None,
        "HDFS_USER": None,
        "LOCAL_STAGING_DIR": None,
    }
    consumer_instance = mocker.Mock()
    mocker.patch.object(run_streaming, "Consumer", return_value=consumer_instance)
    kafka_consumer_class = mocker.patch.object(
        run_streaming, "KafkaConsumer", return_value=mocker.Mock()
    )
    mocker.patch.object(run_streaming, "HDFSClient", return_value=mocker.Mock())
    mocker.patch.object(run_streaming, "configure_logging")
    mocker.patch.object(run_streaming.logging, "getLogger", return_value=mocker.Mock())
    mocker.patch.object(
        run_streaming.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_streaming.main()

    assert result == 0
    assert kafka_consumer_class.call_args.args[0] == "air_quality_varna"
    assert run_streaming.Consumer.call_args.kwargs["city"] == "varna"


@pytest.mark.parametrize("missing_value", [None, ""])
def test_main_uses_default_kafka_bootstrap_servers_when_env_is_missing(
    mocker, missing_value
):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": missing_value,
        "KAFKA_TOPIC": None,
        "CITY": "sofia",
        "OUTPUT_ROOT": None,
        "PROCESSING_DATE": None,
        "HDFS_NAMENODE_URL": None,
        "HDFS_USER": None,
        "LOCAL_STAGING_DIR": None,
    }
    consumer_instance = mocker.Mock()
    kafka_consumer_class = mocker.patch.object(
        run_streaming, "KafkaConsumer", return_value=mocker.Mock()
    )
    mocker.patch.object(run_streaming, "Consumer", return_value=consumer_instance)
    mocker.patch.object(run_streaming, "HDFSClient", return_value=mocker.Mock())
    mocker.patch.object(run_streaming, "configure_logging")
    mocker.patch.object(run_streaming.logging, "getLogger", return_value=mocker.Mock())
    mocker.patch.object(
        run_streaming.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_streaming.main()

    assert result == 0
    kafka_consumer_class.assert_called_once()
    assert kafka_consumer_class.call_args.kwargs["bootstrap_servers"] == [
        run_streaming.DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    ]


def test_create_kafka_consumer_retries_when_broker_is_temporarily_unavailable(mocker):
    run_streaming = _load_run_streaming_module()
    logger = mocker.Mock()
    kafka_consumer = mocker.Mock()
    sleep = mocker.Mock()
    kafka_consumer_class = mocker.patch.object(
        run_streaming,
        "KafkaConsumer",
        side_effect=[NoBrokersAvailable(), kafka_consumer],
    )

    result = run_streaming.create_kafka_consumer(
        kafka_bootstrap_servers="broker-1:9092,broker-2:9092",
        kafka_topic="air_quality_sofia",
        logger=logger,
        retry_attempts=2,
        retry_backoff_seconds=7,
        sleep=sleep,
    )

    assert result is kafka_consumer
    assert kafka_consumer_class.call_count == 2
    assert kafka_consumer_class.call_args.kwargs["bootstrap_servers"] == [
        "broker-1:9092",
        "broker-2:9092",
    ]
    sleep.assert_called_once_with(7)


def test_wait_for_hdfs_retries_until_namenode_is_available(mocker):
    run_streaming = _load_run_streaming_module()
    hdfs_client = mocker.Mock()
    hdfs_client.exists.side_effect = [RuntimeError("not ready"), True]
    logger = mocker.Mock()
    sleep = mocker.Mock()

    run_streaming.wait_for_hdfs(
        hdfs_client=hdfs_client,
        logger=logger,
        retry_attempts=2,
        retry_backoff_seconds=7,
        sleep=sleep,
    )

    assert hdfs_client.exists.call_args_list == [mocker.call("/"), mocker.call("/")]
    sleep.assert_called_once_with(7)
