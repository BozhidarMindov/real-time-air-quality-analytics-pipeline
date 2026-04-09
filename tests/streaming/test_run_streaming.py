import importlib.util
from pathlib import Path


def _load_run_streaming_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "run_streaming.py"
    spec = importlib.util.spec_from_file_location("run_streaming_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module

def test_main_reads_environment_and_forwards_values(mocker):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "KAFKA_TOPIC": "air_quality_varna",
        "CITY": "varna",
        "OUTPUT_ROOT": "/warehouse/air-quality",
        "PROCESSING_DATE": "2026-04-06",
        "HDFS_NAMENODE_URL": "http://namenode:9870",
        "HDFS_USER": "airflow",
        "LOCAL_STAGING_DIR": "/tmp/air-quality",
    }
    consumer_instance = mocker.Mock()
    consumer_class = mocker.patch.object(run_streaming, "Consumer", return_value=consumer_instance)
    configure_logging = mocker.patch.object(run_streaming, "configure_logging")
    get_env_or_default = mocker.patch.object(
        run_streaming,
        "get_env_or_default",
        side_effect=lambda key, default: env_values.get(key) or default,
    )

    result = run_streaming.main()

    assert result == 0
    configure_logging.assert_called_once_with()
    assert get_env_or_default.call_count == 8
    consumer_class.assert_called_once_with(
        aqicn_api_token="",
        city="varna",
        kafka_bootstrap_servers="broker:9092",
        kafka_topic="air_quality_varna",
        output_root="/warehouse/air-quality",
        processing_date="2026-04-06",
        hdfs_namenode_url="http://namenode:9870",
        hdfs_user="airflow",
        local_staging_dir="/tmp/air-quality",
    )
    consumer_instance.run.assert_called_once_with()


def test_main_uses_streaming_defaults_when_env_is_missing_or_blank(mocker):
    run_streaming = _load_run_streaming_module()
    env_values = {
        "KAFKA_BOOTSTRAP_SERVERS": "",
        "KAFKA_TOPIC": None,
        "CITY": None,
        "OUTPUT_ROOT": "",
        "PROCESSING_DATE": "",
        "HDFS_NAMENODE_URL": "",
        "HDFS_USER": None,
        "LOCAL_STAGING_DIR": "",
    }
    consumer_instance = mocker.Mock()
    consumer_class = mocker.patch.object(run_streaming, "Consumer", return_value=consumer_instance)
    mocker.patch.object(run_streaming, "configure_logging")
    mocker.patch.object(
        run_streaming,
        "get_env_or_default",
        side_effect=lambda key, default: env_values.get(key) or default,
    )

    result = run_streaming.main()

    assert result == 0
    consumer_class.assert_called_once_with(
        aqicn_api_token="",
        city="sofia",
        kafka_bootstrap_servers=run_streaming.DEFAULT_BOOTSTRAP_SERVERS,
        kafka_topic=run_streaming.DEFAULT_KAFKA_TOPIC,
        output_root="/data/air-quality",
        processing_date=None,
        hdfs_namenode_url=run_streaming.DEFAULT_HDFS_NAMENODE_URL,
        hdfs_user=run_streaming.DEFAULT_HDFS_USER,
        local_staging_dir=run_streaming.DEFAULT_LOCAL_STAGING_DIR,
    )
    consumer_instance.run.assert_called_once_with()
