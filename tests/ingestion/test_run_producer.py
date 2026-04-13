import importlib.util
from pathlib import Path


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
        "KAFKA_TOPIC": "air_quality_varna",
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
    assert getenv.call_count == 9
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


def test_main_uses_defaults_when_producer_env_is_missing_or_blank(mocker):
    run_producer = _load_run_producer_module()
    env_values = {
        "AQICN_API_TOKEN": "",
        "CITY": None,
        "POLL_INTERVAL_SECONDS": None,
        "KAFKA_BOOTSTRAP_SERVERS": "",
        "KAFKA_TOPIC": "",
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
        api_token="",
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
        poll_interval_seconds=60,
        kafka_topic="air_quality_sofia",
    )
    producer_instance.run.assert_called_once_with()
