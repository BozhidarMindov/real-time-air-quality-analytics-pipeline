from src.analytics import batch_analysis


def test_build_curated_input_path_uses_city_jsonl_glob():
    result = batch_analysis.build_curated_input_path("/data/air-quality", "sofia")

    assert result == "hdfs://namenode:9000/data/air-quality/sofia/curated/*.jsonl"


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
                "pollutants": {"pm10": 20.0, "no2": 10.0, "o3": 15.0},
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
                "pollutants": {"pm10": 35.0, "pm25": 14.0},
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


def test_normalize_curated_dataframe_adds_hour_and_day_columns(spark_session):
    source = spark_session.createDataFrame(
        [
            {
                "timestamp": "2026-04-07T10:15:00+03:00",
                "aqi": 64,
                "pollutants": {"pm10": 31.5},
            }
        ]
    )

    result = batch_analysis.normalize_curated_dataframe(source).collect()[0]

    assert result["aqi"] == 64
    assert result["hour"] == 10
    assert result["day"] == "2026-04-07"


def test_normalize_curated_dataframe_filters_rows_with_invalid_timestamp_or_missing_aqi(
    spark_session,
):
    source = spark_session.createDataFrame(
        [
            {
                "timestamp": "not-a-timestamp",
                "aqi": 64,
            },
            {
                "timestamp": "2026-04-07T10:15:00+03:00",
                "aqi": None,
            },
            {
                "timestamp": "2026-04-07T11:30:00+03:00",
                "aqi": 72,
            },
        ]
    )

    rows = [
        row.asDict()
        for row in batch_analysis.normalize_curated_dataframe(source).collect()
    ]

    assert len(rows) == 1
    assert rows[0]["aqi"] == 72
    assert rows[0]["hour"] == 11
    assert rows[0]["day"] == "2026-04-07"
