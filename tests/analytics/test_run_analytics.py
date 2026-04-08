import importlib.util
from pathlib import Path

from src.analytics import batch_analysis
from src.reporting import notebook_helpers


def _load_run_analytics_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "run_analytics.py"
    spec = importlib.util.spec_from_file_location("run_analytics_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_batch_analysis_returns_expected_summary_keys(spark_session, mocker):
    source = spark_session.createDataFrame(
        [
            {
                "timestamp": "2026-04-07T10:00:00+03:00",
                "station_id": 1,
                "station_name": "Sofia",
                "latitude": 42.6977,
                "longitude": 23.3219,
                "aqi": 40,
                "dominant_pollutant": "pm10",
                "pm10": 20.0,
                "no2": 10.0,
                "o3": 15.0,
                "temperature": 18.0,
                "humidity": 50.0,
                "wind": 2.0,
                "pressure": 1008.0,
                "dew": 7.0,
            },
            {
                "timestamp": "2026-04-07T11:00:00+03:00",
                "station_id": 1,
                "station_name": "Sofia",
                "latitude": 42.6977,
                "longitude": 23.3219,
                "aqi": 95,
                "dominant_pollutant": "pm10",
                "pm10": 35.0,
                "no2": 12.0,
                "o3": 18.0,
                "temperature": 19.0,
                "humidity": 48.0,
                "wind": 2.5,
                "pressure": 1007.0,
                "dew": 8.0,
            },
        ]
    )
    mocker.patch.object(batch_analysis, "load_curated_dataframe", return_value=source)

    result = batch_analysis.run_batch_analysis(
        spark_session,
        "/data/air-quality",
        "sofia",
        aqi_threshold=90,
        jump_threshold=30,
    )

    assert set(result) == {
        "normalized",
        "hourly_aqi",
        "average_pollutants",
        "dominant_pollutants",
        "weather_correlations",
        "aqi_spikes",
    }


def test_to_pandas_table_returns_pandas_dataframe(spark_session):
    source = spark_session.createDataFrame([{"hour": 10, "avg_aqi": 70.0}])

    result = notebook_helpers.to_pandas_table(source)

    assert result.to_dict("records") == [{"hour": 10, "avg_aqi": 70.0}]


def test_main_reads_environment_and_calls_run_batch_analysis(mocker):
    run_analytics = _load_run_analytics_module()
    logger = mocker.Mock()
    spark_session = mocker.Mock()
    mocker.patch.object(run_analytics.logging, "basicConfig")
    mocker.patch.object(run_analytics.logging, "getLogger", return_value=logger)
    mocker.patch.object(run_analytics, "create_spark_session", return_value=spark_session)
    run_batch_analysis = mocker.patch.object(
        run_analytics,
        "run_batch_analysis",
        return_value={
            "normalized": mocker.Mock(count=mocker.Mock(return_value=2)),
            "hourly_aqi": mocker.Mock(),
            "average_pollutants": mocker.Mock(),
            "dominant_pollutants": mocker.Mock(),
            "weather_correlations": mocker.Mock(),
            "aqi_spikes": mocker.Mock(count=mocker.Mock(return_value=1)),
        },
    )
    env_values = {
        "CITY": "sofia",
        "OUTPUT_ROOT": "/data/air-quality",
        "AQI_SPIKE_THRESHOLD": "90",
        "AQI_JUMP_THRESHOLD": "30",
    }
    mocker.patch.object(run_analytics.os, "getenv", side_effect=lambda key, default=None: env_values.get(key, default))

    result = run_analytics.main()

    assert result == 0
    run_batch_analysis.assert_called_once_with(
        spark_session,
        output_root="/data/air-quality",
        city="sofia",
        aqi_threshold=90,
        jump_threshold=30,
    )
    spark_session.stop.assert_called_once_with()
