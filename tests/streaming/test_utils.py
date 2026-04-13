from src.streaming.utils import build_daily_output_path
from src.streaming.utils import join_hdfs_root
from src.streaming.utils import normalize_hdfs_path
from src.streaming.utils import serialize_jsonl


def test_join_hdfs_root_normalizes_slashes():
    result = join_hdfs_root("data\\air-quality\\", "/sofia/", "raw")

    assert result == "/data/air-quality/sofia/raw"


def test_normalize_hdfs_path_normalizes_hdfs_uri_with_backslashes():
    result = normalize_hdfs_path("hdfs://namenode:9000\\data\\air-quality\\")

    assert result == "/data/air-quality"


def test_build_daily_output_path_builds_jsonl_path_for_city_and_day():
    result = build_daily_output_path(
        "/data/air-quality", "sofia", "curated", "2026-04-07"
    )

    assert result == "/data/air-quality/sofia/curated/2026-04-07.jsonl"


def test_serialize_jsonl_returns_compact_json_lines():
    result = serialize_jsonl(
        [
            {"city": "sofia", "aqi": 42},
            {"city": "varna", "aqi": None},
        ]
    )

    assert result == ('{"city":"sofia","aqi":42}\n{"city":"varna","aqi":null}\n')
