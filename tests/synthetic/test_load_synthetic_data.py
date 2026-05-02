import importlib.util
import json
from datetime import datetime
from datetime import timezone
from pathlib import Path


def _load_module():
    module_path = (
        Path(__file__).resolve().parents[2] / "scripts" / "load_synthetic_data.py"
    )
    spec = importlib.util.spec_from_file_location(
        "load_synthetic_data_test_module", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_main_publishes_synthetic_payloads_to_city_topic(mocker):
    load_synthetic_data = _load_module()
    env_values = {
        "CITY": "varna",
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "SYNTHETIC_DAYS": "1",
        "SYNTHETIC_INTERVAL_MINUTES": "60",
        "SYNTHETIC_STATION_COUNT": "2",
    }
    kafka_producer = mocker.Mock()
    kafka_producer_class = mocker.patch.object(
        load_synthetic_data, "KafkaProducer", return_value=kafka_producer
    )
    mocker.patch.object(load_synthetic_data, "load_dotenv")
    mocker.patch.object(load_synthetic_data, "configure_logging")
    mocker.patch.object(
        load_synthetic_data.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )
    mocker.patch.object(
        load_synthetic_data,
        "utc_now",
        return_value=datetime(2026, 4, 11, tzinfo=timezone.utc),
    )

    result = load_synthetic_data.main()

    assert result == 0
    kafka_producer_class.assert_called_once_with(bootstrap_servers=["broker:9092"])
    assert kafka_producer.send.call_count == 48
    assert kafka_producer.send.call_args.args[0] == "air_quality_varna"
    kafka_producer.flush.assert_called_once_with()
    kafka_producer.close.assert_called_once_with()


def test_main_defaults_to_hourly_single_station_fifteen_day_dataset(mocker):
    load_synthetic_data = _load_module()
    env_values = {
        "CITY": "varna",
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
    }
    kafka_producer = mocker.Mock()
    mocker.patch.object(
        load_synthetic_data, "KafkaProducer", return_value=kafka_producer
    )
    mocker.patch.object(load_synthetic_data, "load_dotenv")
    mocker.patch.object(load_synthetic_data, "configure_logging")
    mocker.patch.object(
        load_synthetic_data.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )
    mocker.patch.object(
        load_synthetic_data,
        "utc_now",
        return_value=datetime(2026, 4, 11, tzinfo=timezone.utc),
    )

    result = load_synthetic_data.main()

    assert result == 0
    assert kafka_producer.send.call_count == 360
    sent_payloads = [
        json.loads(call.kwargs["value"].decode("utf-8"))
        for call in kafka_producer.send.call_args_list
    ]
    station_ids = {payload["data"]["idx"] for payload in sent_payloads}
    timestamps = [payload["data"]["time"]["iso"] for payload in sent_payloads]
    assert station_ids == {10001}
    assert timestamps[0] == "2026-03-27T00:00:00+00:00"
    assert timestamps[-1] == "2026-04-10T23:00:00+00:00"


def test_main_aligns_default_synthetic_timestamps_to_top_of_hour(mocker):
    load_synthetic_data = _load_module()
    env_values = {
        "CITY": "varna",
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
    }
    kafka_producer = mocker.Mock()
    mocker.patch.object(
        load_synthetic_data, "KafkaProducer", return_value=kafka_producer
    )
    mocker.patch.object(load_synthetic_data, "load_dotenv")
    mocker.patch.object(load_synthetic_data, "configure_logging")
    mocker.patch.object(
        load_synthetic_data.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )
    mocker.patch.object(
        load_synthetic_data,
        "utc_now",
        return_value=datetime(2026, 4, 11, 13, 55, tzinfo=timezone.utc),
    )

    load_synthetic_data.main()

    first_payload = json.loads(
        kafka_producer.send.call_args_list[0].kwargs["value"].decode("utf-8")
    )
    assert first_payload["data"]["time"]["iso"] == "2026-03-27T13:00:00+00:00"
