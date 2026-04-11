from src.analytics import metrics


def test_compute_average_aqi_by_hour_of_day_returns_average_by_hour(spark_session):
    source = spark_session.createDataFrame(
        [
            {"hour": 10, "aqi": 60},
            {"hour": 10, "aqi": 80},
            {"hour": 11, "aqi": 50},
        ]
    )

    rows = [row.asDict() for row in metrics.compute_average_aqi_by_hour_of_day(source).collect()]

    assert rows == [
        {"hour": 10, "avg_aqi": 70.0},
        {"hour": 11, "avg_aqi": 50.0},
    ]


def test_compute_average_aqi_by_hour_of_day_ignores_null_hour_buckets(spark_session):
    source = spark_session.createDataFrame(
        [
            {"hour": None, "aqi": 999},
            {"hour": 10, "aqi": 60},
            {"hour": 10, "aqi": 80},
        ]
    )

    rows = [row.asDict() for row in metrics.compute_average_aqi_by_hour_of_day(source).collect()]

    assert rows == [{"hour": 10, "avg_aqi": 70.0}]


def test_compute_average_pollutants_returns_pm10_no2_o3(spark_session):
    source = spark_session.createDataFrame(
        [
            {"pm10": 30.0, "no2": 10.0, "o3": 20.0},
            {"pm10": 50.0, "no2": 20.0, "o3": 40.0},
        ]
    )

    row = metrics.compute_average_pollutants(source).collect()[0]

    assert row.asDict() == {
        "avg_pm10": 40.0,
        "avg_no2": 15.0,
        "avg_o3": 30.0,
    }


def test_compute_dominant_pollutant_counts_orders_by_frequency(spark_session):
    source = spark_session.createDataFrame(
        [
            {"dominant_pollutant": "pm10"},
            {"dominant_pollutant": "pm10"},
            {"dominant_pollutant": "no2"},
            {"dominant_pollutant": None},
        ]
    )

    rows = [row.asDict() for row in metrics.compute_dominant_pollutant_counts(source).collect()]

    assert rows == [
        {"dominant_pollutant": "pm10", "count": 2},
        {"dominant_pollutant": "no2", "count": 1},
    ]


def test_compute_weather_correlations_returns_null_for_constant_weather_series(spark_session):
    source = spark_session.createDataFrame(
        [
            {"aqi": 10.0, "temperature": 20.0, "humidity": 30.0, "wind": 1.0},
            {"aqi": 20.0, "temperature": 21.0, "humidity": 30.0, "wind": 1.0},
            {"aqi": 30.0, "temperature": 22.0, "humidity": 30.0, "wind": 1.0},
        ]
    )

    row = metrics.compute_weather_correlations(source).collect()[0]

    assert row.asDict() == {
        "aqi_temperature_corr": 1.0,
        "aqi_humidity_corr": None,
        "aqi_wind_corr": None,
    }
