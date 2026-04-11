from src.analytics import batch_analysis


def test_build_curated_input_path_uses_city_jsonl_glob():
    result = batch_analysis.build_curated_input_path("/data/air-quality", "sofia")

    assert result == "hdfs://namenode:9000/data/air-quality/sofia/curated/*.jsonl"


def test_normalize_curated_dataframe_adds_hour_and_day_columns(spark_session):
    source = spark_session.createDataFrame(
        [
            {
                "timestamp": "2026-04-07T10:15:00+03:00",
                "aqi": 64,
                "pm10": 31.5,
            }
        ]
    )

    result = batch_analysis.normalize_curated_dataframe(source).collect()[0]

    assert result["aqi"] == 64
    assert result["hour"] == 10
    assert result["day"] == "2026-04-07"


def test_normalize_curated_dataframe_filters_rows_with_invalid_timestamp_or_missing_aqi(spark_session):
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

    rows = [row.asDict() for row in batch_analysis.normalize_curated_dataframe(source).collect()]

    assert len(rows) == 1
    assert rows[0]["aqi"] == 72
    assert rows[0]["hour"] == 11
    assert rows[0]["day"] == "2026-04-07"
