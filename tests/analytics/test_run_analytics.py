import importlib.util
from pathlib import Path

import pytest

from src.analytics import batch_analysis


def _load_run_analytics_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "run_analytics.py"
    spec = importlib.util.spec_from_file_location(
        "run_analytics_test_module", module_path
    )
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
    )

    assert set(result) == {
        "normalized",
        "hourly_aqi",
        "daily_aqi",
        "aqi_category_distribution",
        "average_pollutants",
        "dominant_pollutants",
        "weather_correlations",
    }


def test_main_reads_environment_and_calls_run_batch_analysis(mocker):
    run_analytics = _load_run_analytics_module()
    logger = mocker.Mock()
    spark_session = mocker.Mock()
    configure_logging = mocker.patch.object(run_analytics, "configure_logging")
    mocker.patch.object(run_analytics.logging, "getLogger", return_value=logger)
    mocker.patch.object(
        run_analytics, "create_spark_session", return_value=spark_session
    )
    run_batch_analysis = mocker.patch.object(
        run_analytics,
        "run_batch_analysis",
        return_value={
            "normalized": mocker.Mock(count=mocker.Mock(return_value=2)),
            "hourly_aqi": mocker.Mock(),
            "daily_aqi": mocker.Mock(),
            "aqi_category_distribution": mocker.Mock(),
            "average_pollutants": mocker.Mock(),
            "dominant_pollutants": mocker.Mock(),
            "weather_correlations": mocker.Mock(),
        },
    )
    env_values = {
        "CITY": "sofia",
        "OUTPUT_ROOT": "/data/air-quality",
    }
    getenv = mocker.patch.object(
        run_analytics.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )
    result = run_analytics.main()

    assert result == 0
    configure_logging.assert_called_once_with()
    assert getenv.call_count == 2
    run_batch_analysis.assert_called_once_with(
        spark_session,
        output_root="/data/air-quality",
        city="sofia",
    )
    spark_session.stop.assert_called_once_with()


@pytest.mark.parametrize("missing_value", [None, ""])
def test_main_raises_when_city_is_missing(mocker, missing_value):
    run_analytics = _load_run_analytics_module()
    mocker.patch.object(run_analytics, "configure_logging")
    create_spark_session = mocker.patch.object(run_analytics, "create_spark_session")
    env_values = {
        "CITY": missing_value,
        "OUTPUT_ROOT": "/data/air-quality",
    }
    mocker.patch.object(
        run_analytics.os,
        "getenv",
        side_effect=lambda key: env_values.get(key),
    )

    with pytest.raises(ValueError, match="CITY is required"):
        run_analytics.main()

    create_spark_session.assert_not_called()
