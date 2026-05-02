import importlib.util
from pathlib import Path

import pytest
from kafka.errors import NoBrokersAvailable


def _load_run_producer_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "run_producer.py"
    spec = importlib.util.spec_from_file_location(
        "run_producer_test_module", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_main_reads_environment_and_starts_producer(mocker):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "token",
        "CITY": "varna",
        "POLL_INTERVAL_SECONDS": "120",
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "AQICN_BASE_URL": "https://example.test/feed",
        "REQUEST_TIMEOUT_SECONDS": "10",
        "RETRY_ATTEMPTS": "7",
        "RETRY_BACKOFF_SECONDS": "9",
    }
    producer_instance = mocker.Mock()
    aqicn_client = mocker.Mock()
    kafka_producer = mocker.Mock()
    producer_class = mocker.patch.object(
        run_producer, "Producer", return_value=producer_instance
    )
    aqicn_client_class = mocker.patch.object(
        run_producer, "AQICNClient", return_value=aqicn_client
    )
    kafka_producer_class = mocker.patch.object(
        run_producer, "KafkaProducer", return_value=kafka_producer
    )
    load_dotenv = mocker.patch.object(run_producer, "load_dotenv")
    configure_logging = mocker.patch.object(run_producer, "configure_logging")
    getenv = mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_producer.main()

    assert result == 0
    load_dotenv.assert_called_once_with(dotenv_path=run_producer.DEFAULT_ENV_PATH)
    configure_logging.assert_called_once_with()
    assert getenv.call_count == 8
    aqicn_client_class.assert_called_once_with(
        api_token="token",
        base_url="https://example.test/feed",
        request_timeout_seconds=10,
        retry_attempts=7,
        retry_backoff_seconds=9,
    )
    kafka_producer_class.assert_called_once_with(bootstrap_servers=["broker:9092"])
    producer_class.assert_called_once_with(
        aqicn_client=aqicn_client,
        kafka_producer=kafka_producer,
        city="varna",
        poll_interval_seconds=120,
        kafka_topic="air_quality_varna",
    )
    producer_instance.run.assert_called_once_with()


def test_main_uses_non_required_defaults_when_optional_producer_env_is_missing_or_blank(
    mocker,
):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "token",
        "CITY": "sofia",
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": "",
        "AQICN_BASE_URL": None,
        "REQUEST_TIMEOUT_SECONDS": None,
        "RETRY_ATTEMPTS": None,
        "RETRY_BACKOFF_SECONDS": None,
    }
    producer_instance = mocker.Mock()
    aqicn_client = mocker.Mock()
    kafka_producer = mocker.Mock()
    producer_class = mocker.patch.object(
        run_producer, "Producer", return_value=producer_instance
    )
    aqicn_client_class = mocker.patch.object(
        run_producer, "AQICNClient", return_value=aqicn_client
    )
    kafka_producer_class = mocker.patch.object(
        run_producer, "KafkaProducer", return_value=kafka_producer
    )
    mocker.patch.object(run_producer, "load_dotenv")
    mocker.patch.object(run_producer, "configure_logging")
    mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_producer.main()

    assert result == 0
    aqicn_client_class.assert_called_once_with(
        api_token="token",
        base_url="https://api.waqi.info/feed",
        request_timeout_seconds=30,
        retry_attempts=3,
        retry_backoff_seconds=5,
    )
    kafka_producer_class.assert_called_once_with(bootstrap_servers=["localhost:9094"])
    producer_class.assert_called_once_with(
        aqicn_client=aqicn_client,
        kafka_producer=kafka_producer,
        city="sofia",
        poll_interval_seconds=300,
        kafka_topic="air_quality_sofia",
    )
    producer_instance.run.assert_called_once_with()


@pytest.mark.parametrize("missing_value", [None, ""])
def test_main_raises_when_city_is_missing(mocker, missing_value):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "token",
        "CITY": missing_value,
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": None,
        "AQICN_BASE_URL": None,
        "REQUEST_TIMEOUT_SECONDS": None,
        "RETRY_ATTEMPTS": None,
        "RETRY_BACKOFF_SECONDS": None,
    }
    mocker.patch.object(run_producer, "load_dotenv")
    mocker.patch.object(run_producer, "configure_logging")
    mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    with pytest.raises(ValueError, match="CITY is required"):
        run_producer.main()


@pytest.mark.parametrize("missing_value", [None, ""])
def test_main_raises_when_aqicn_api_token_is_missing(mocker, missing_value):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": missing_value,
        "CITY": "sofia",
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": None,
        "AQICN_BASE_URL": None,
        "REQUEST_TIMEOUT_SECONDS": None,
        "RETRY_ATTEMPTS": None,
        "RETRY_BACKOFF_SECONDS": None,
    }
    mocker.patch.object(run_producer, "load_dotenv")
    mocker.patch.object(run_producer, "configure_logging")
    mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    with pytest.raises(ValueError, match="AQICN_API_TOKEN is required"):
        run_producer.main()


def test_main_builds_kafka_topic_from_city_when_topic_env_is_missing(mocker):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "token",
        "CITY": "varna",
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",
        "KAFKA_TOPIC": "",
        "AQICN_BASE_URL": None,
        "REQUEST_TIMEOUT_SECONDS": None,
        "RETRY_ATTEMPTS": None,
        "RETRY_BACKOFF_SECONDS": None,
    }
    producer_instance = mocker.Mock()
    mocker.patch.object(run_producer, "Producer", return_value=producer_instance)
    mocker.patch.object(run_producer, "AQICNClient")
    mocker.patch.object(run_producer, "KafkaProducer")
    mocker.patch.object(run_producer, "load_dotenv")
    mocker.patch.object(run_producer, "configure_logging")
    mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_producer.main()

    assert result == 0
    run_producer.Producer.assert_called_once()
    assert run_producer.Producer.call_args.kwargs["city"] == "varna"
    assert run_producer.Producer.call_args.kwargs["kafka_topic"] == "air_quality_varna"


def test_main_ignores_kafka_topic_env_and_builds_topic_from_city(mocker):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "token",
        "CITY": "varna",
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",
        "KAFKA_TOPIC": "custom_topic",
        "AQICN_BASE_URL": None,
        "REQUEST_TIMEOUT_SECONDS": None,
        "RETRY_ATTEMPTS": None,
        "RETRY_BACKOFF_SECONDS": None,
    }
    producer_instance = mocker.Mock()
    mocker.patch.object(run_producer, "Producer", return_value=producer_instance)
    mocker.patch.object(run_producer, "AQICNClient")
    mocker.patch.object(run_producer, "KafkaProducer")
    mocker.patch.object(run_producer, "load_dotenv")
    mocker.patch.object(run_producer, "configure_logging")
    mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_producer.main()

    assert result == 0
    assert run_producer.Producer.call_args.kwargs["kafka_topic"] == "air_quality_varna"


@pytest.mark.parametrize("missing_value", [None, ""])
def test_main_uses_default_kafka_bootstrap_servers_when_env_is_missing(
    mocker, missing_value
):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "token",
        "CITY": "sofia",
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": missing_value,
        "AQICN_BASE_URL": None,
        "REQUEST_TIMEOUT_SECONDS": None,
        "RETRY_ATTEMPTS": None,
        "RETRY_BACKOFF_SECONDS": None,
    }
    producer_instance = mocker.Mock()
    mocker.patch.object(run_producer, "Producer", return_value=producer_instance)
    mocker.patch.object(run_producer, "AQICNClient")
    kafka_producer_class = mocker.patch.object(
        run_producer, "KafkaProducer", return_value=mocker.Mock()
    )
    mocker.patch.object(run_producer, "load_dotenv")
    mocker.patch.object(run_producer, "configure_logging")
    mocker.patch.object(run_producer.logging, "getLogger", return_value=mocker.Mock())
    mocker.patch.object(
        run_producer.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    result = run_producer.main()

    assert result == 0
    kafka_producer_class.assert_called_once_with(
        bootstrap_servers=[run_producer.DEFAULT_KAFKA_BOOTSTRAP_SERVERS]
    )


def test_create_kafka_producer_retries_when_broker_is_temporarily_unavailable(mocker):
    run_producer = _load_run_producer_module()
    logger = mocker.Mock()
    kafka_producer = mocker.Mock()
    sleep = mocker.Mock()
    kafka_producer_class = mocker.patch.object(
        run_producer,
        "KafkaProducer",
        side_effect=[NoBrokersAvailable(), kafka_producer],
    )

    result = run_producer.create_kafka_producer(
        kafka_bootstrap_servers="broker-1:9092,broker-2:9092",
        logger=logger,
        retry_attempts=2,
        retry_backoff_seconds=7,
        sleep=sleep,
    )

    assert result is kafka_producer
    assert kafka_producer_class.call_count == 2
    assert kafka_producer_class.call_args.kwargs["bootstrap_servers"] == [
        "broker-1:9092",
        "broker-2:9092",
    ]
    sleep.assert_called_once_with(7)
