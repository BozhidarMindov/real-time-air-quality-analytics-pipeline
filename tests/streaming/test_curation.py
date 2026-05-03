import json

import pytest

from src.streaming.curation import extract_curated_record
from src.streaming.curation import extract_day
from src.streaming.curation import filter_curated_records
from src.streaming.curation import get_geo_value
from src.streaming.curation import load_curated_observation_cache
from src.streaming.curation import persist_curated_observation_cache


def test_extract_curated_record_keeps_only_required_fields():
    payload = {
        "data": {
            "idx": 42,
            "aqi": 64,
            "dominentpol": "pm10",
            "city": {"name": "Sofia", "geo": [42.6977, 23.3219]},
            "time": {"iso": "2026-04-07T10:00:00+03:00"},
            "iaqi": {
                "pm10": {"v": 31.5},
                "pm25": {"v": 14.7},
                "no2": {"v": 18.2},
                "o3": {"v": 11.4},
                "co": {"v": 2.1},
                "t": {"v": 19.1},
                "h": {"v": 47.0},
                "w": {"v": 3.5},
                "p": {"v": 1008.0},
                "dew": {"v": 7.2},
            },
        },
        "ignored": {"nested": "value"},
    }

    result = extract_curated_record(payload)

    assert result == {
        "timestamp": "2026-04-07T10:00:00+03:00",
        "station_id": 42,
        "station_name": "Sofia",
        "latitude": 42.6977,
        "longitude": 23.3219,
        "aqi": 64,
        "dominant_pollutant": "pm10",
        "pollutants": {
            "co": 2.1,
            "no2": 18.2,
            "o3": 11.4,
            "pm10": 31.5,
            "pm25": 14.7,
        },
        "temperature": 19.1,
        "humidity": 47.0,
        "wind": 3.5,
        "pressure": 1008.0,
        "dew": 7.2,
    }
    assert "pm10" not in result
    assert "no2" not in result
    assert "o3" not in result


def test_filter_curated_records_skips_cached_and_batch_duplicates(tmp_path, mocker):
    cache_path = tmp_path / "curated_observation_cache.json"
    cache_path.write_text('{"42":"2026-04-07T10:00:00+03:00"}', encoding="utf-8")
    logger = mocker.Mock()
    curated_records = [
        {"timestamp": "2026-04-07T10:00:00+03:00", "station_id": 42},
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": 42},
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": 42},
    ]

    filtered_records, updated_cache = filter_curated_records(
        curated_records,
        load_curated_observation_cache(cache_path),
        logger=logger,
    )

    assert filtered_records == [
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": 42}
    ]
    assert updated_cache == {"42": "2026-04-07T11:00:00+03:00"}
    logger.warning.assert_not_called()
    logger.info.assert_any_call(
        "Skipping duplicate curated record for station_id=42 timestamp=2026-04-07T10:00:00+03:00"
    )
    logger.info.assert_any_call(
        "Skipping duplicate curated record for station_id=42 timestamp=2026-04-07T11:00:00+03:00"
    )


def test_filter_curated_records_skips_missing_dedup_fields(mocker):
    logger = mocker.Mock()
    curated_records = [
        {"timestamp": "2026-04-07T10:00:00+03:00", "station_id": 42},
        {"timestamp": "2026-04-07T11:00:00+03:00", "station_id": None},
        {"timestamp": None, "station_id": 42},
    ]

    filtered_records, updated_cache = filter_curated_records(
        curated_records,
        {},
        logger=logger,
    )

    assert filtered_records == [
        {"timestamp": "2026-04-07T10:00:00+03:00", "station_id": 42}
    ]
    assert updated_cache == {"42": "2026-04-07T10:00:00+03:00"}
    assert logger.warning.call_count == 2


def test_load_curated_observation_cache_raises_for_invalid_persisted_file(tmp_path):
    (tmp_path / "curated_observation_cache.json").write_text(
        "not-json", encoding="utf-8"
    )

    with pytest.raises(json.JSONDecodeError):
        load_curated_observation_cache(tmp_path / "curated_observation_cache.json")


def test_persist_curated_observation_cache_uses_atomic_replace(tmp_path, mocker):
    path_type = type(tmp_path)
    replace_spy = mocker.spy(path_type, "replace")

    persist_curated_observation_cache(
        tmp_path / "curated_observation_cache.json",
        {"42": "2026-04-07T10:00:00+03:00"},
    )

    cache_path = tmp_path / "curated_observation_cache.json"
    temp_path = tmp_path / "curated_observation_cache.json.tmp"
    assert cache_path.read_text(encoding="utf-8") == (
        '{"42":"2026-04-07T10:00:00+03:00"}'
    )
    assert temp_path.exists() is False
    replace_spy.assert_called_once_with(temp_path, cache_path)


def test_get_geo_value_returns_requested_coordinate():
    result = get_geo_value({"city": {"geo": [42.6977, 23.3219]}}, 1)

    assert result == 23.3219


def test_extract_day_falls_back_to_processing_date():
    result = extract_day({"data": {"idx": 7}}, "2026-04-07")

    assert result == "2026-04-07"
