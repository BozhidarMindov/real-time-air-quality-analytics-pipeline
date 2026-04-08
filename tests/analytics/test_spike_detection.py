from datetime import datetime

from src.analytics import metrics
from src.analytics import spike_detection


def test_compute_weather_correlations_returns_expected_columns(spark_session):
    source = spark_session.createDataFrame(
        [
            {"aqi": 10.0, "temperature": 20.0, "humidity": 30.0, "wind": 1.0},
            {"aqi": 20.0, "temperature": 25.0, "humidity": 35.0, "wind": 2.0},
        ]
    )

    row = metrics.compute_weather_correlations(source).collect()[0]

    assert set(row.asDict()) == {
        "aqi_temperature_corr",
        "aqi_humidity_corr",
        "aqi_wind_corr",
    }


def test_detect_aqi_spikes_marks_threshold_and_jump_spikes(spark_session):
    source = spark_session.createDataFrame(
        [
            {"station_id": 1, "event_timestamp": datetime(2026, 4, 7, 10, 0, 0), "aqi": 40},
            {"station_id": 1, "event_timestamp": datetime(2026, 4, 7, 11, 0, 0), "aqi": 95},
            {"station_id": 1, "event_timestamp": datetime(2026, 4, 7, 12, 0, 0), "aqi": 100},
        ]
    )

    rows = [
        row.asDict()
        for row in spike_detection.detect_aqi_spikes(
            source,
            aqi_threshold=90,
            jump_threshold=30,
        ).collect()
    ]

    assert rows == [
        {
            "station_id": 1,
            "event_timestamp": datetime(2026, 4, 7, 11, 0, 0),
            "aqi": 95,
            "previous_aqi": 40,
            "aqi_jump": 55,
            "is_threshold_spike": True,
            "is_jump_spike": True,
        },
        {
            "station_id": 1,
            "event_timestamp": datetime(2026, 4, 7, 12, 0, 0),
            "aqi": 100,
            "previous_aqi": 95,
            "aqi_jump": 5,
            "is_threshold_spike": True,
            "is_jump_spike": False,
        },
    ]
