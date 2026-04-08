import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace


def _load_run_streaming_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "run_streaming.py"
    spec = importlib.util.spec_from_file_location("run_streaming_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _build_fake_pandas_module():
    pandas_module = ModuleType("pandas")
    pandas_module.DataFrame = type("FakeDataFrame", (), {"from_records": staticmethod(lambda records, columns: object())})
    return pandas_module


def _load_streaming_job_module(mocker):
    module_path = Path(__file__).resolve().parents[2] / "src" / "streaming" / "streaming_job.py"
    spec = importlib.util.spec_from_file_location("streaming_job_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    mocker.patch.dict(sys.modules, {"pandas": _build_fake_pandas_module()})
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_load_streaming_module_imports_the_single_job_module(mocker):
    run_streaming = _load_run_streaming_module()
    import_module = mocker.patch.object(run_streaming.importlib, "import_module", return_value="loaded")

    result = run_streaming._load_streaming_module()

    assert result == "loaded"
    import_module.assert_called_once_with("src.streaming.streaming_job")


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
    streaming_main = mocker.Mock(return_value=0)
    streaming_module = SimpleNamespace(
        main=streaming_main,
        DEFAULT_BOOTSTRAP_SERVERS="localhost:9094",
        DEFAULT_KAFKA_TOPIC="air_quality_sofia",
        DEFAULT_CITY="sofia",
        DEFAULT_OUTPUT_ROOT="/data/air-quality",
        DEFAULT_HDFS_NAMENODE_URL="http://namenode:9870",
        DEFAULT_HDFS_USER="hdfs",
        DEFAULT_LOCAL_STAGING_DIR="/tmp/streaming",
    )

    mocker.patch.object(run_streaming, "os", create=True)
    mocker.patch.object(run_streaming.os, "getenv", side_effect=lambda key, default=None: env_values.get(key, default))
    mocker.patch.object(run_streaming, "_load_streaming_module", return_value=streaming_module)

    result = run_streaming.main()

    assert result == 0
    streaming_main.assert_called_once_with(
        bootstrap_servers="broker:9092",
        topic="air_quality_varna",
        city="varna",
        output_root="/warehouse/air-quality",
        processing_date="2026-04-06",
        hdfs_namenode_url="http://namenode:9870",
        hdfs_user="airflow",
        local_staging_dir="/tmp/air-quality",
    )


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
    streaming_main = mocker.Mock(return_value=0)
    streaming_module = SimpleNamespace(
        main=streaming_main,
        DEFAULT_BOOTSTRAP_SERVERS="broker:9092",
        DEFAULT_KAFKA_TOPIC="air_quality_sofia",
        DEFAULT_CITY="sofia",
        DEFAULT_OUTPUT_ROOT="/data/air-quality",
        DEFAULT_HDFS_NAMENODE_URL="http://namenode:9870",
        DEFAULT_HDFS_USER="hdfs",
        DEFAULT_LOCAL_STAGING_DIR="/tmp/streaming",
    )

    mocker.patch.object(run_streaming, "os", create=True)
    mocker.patch.object(run_streaming.os, "getenv", side_effect=lambda key, default=None: env_values.get(key, default))
    mocker.patch.object(run_streaming, "_load_streaming_module", return_value=streaming_module)

    result = run_streaming.main()

    assert result == 0
    streaming_main.assert_called_once_with(
        bootstrap_servers="broker:9092",
        topic="air_quality_sofia",
        city="sofia",
        output_root="/data/air-quality",
        processing_date=None,
        hdfs_namenode_url="http://namenode:9870",
        hdfs_user="hdfs",
        local_staging_dir="/tmp/streaming",
    )


def test_run_cli_calls_main(mocker):
    run_streaming = _load_run_streaming_module()
    main_mock = mocker.patch.object(run_streaming, "main", return_value=0)

    result = run_streaming.run_cli()

    assert result == 0
    main_mock.assert_called_once_with()


def test_streaming_job_main_builds_consumer_and_runs(mocker):
    streaming_job = _load_streaming_job_module(mocker)
    consumer_class = mocker.patch.object(streaming_job, "Consumer")

    result = streaming_job.main(
        bootstrap_servers="broker:9092",
        topic="air_quality_sofia",
        city="sofia",
        output_root="/data/air-quality",
        processing_date="2026-04-07",
        hdfs_namenode_url="http://namenode:9870",
        hdfs_user="hdfs",
        local_staging_dir="/tmp/air-quality",
    )

    assert result == 0
    consumer_class.assert_called_once_with(
        aqicn_api_token="",
        city="sofia",
        kafka_bootstrap_servers="broker:9092",
        kafka_topic="air_quality_sofia",
        output_root="/data/air-quality",
        processing_date="2026-04-07",
        hdfs_namenode_url="http://namenode:9870",
        hdfs_user="hdfs",
        local_staging_dir="/tmp/air-quality",
    )
    consumer_class.return_value.run.assert_called_once_with()
