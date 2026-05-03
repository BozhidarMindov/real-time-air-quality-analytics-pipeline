from datetime import datetime
from datetime import timezone

from src.streaming.curation import extract_curated_record
from src.synthetic.generator import generate_synthetic_payloads
from src.synthetic.generator import load_example_payload


def test_load_example_payload_reads_first_raw_jsonl_record():
    payload = load_example_payload()

    assert payload["status"] == "ok"
    assert payload["data"]["idx"] == 8086
    assert payload["data"]["time"]["iso"] == "2026-05-02T04:00:00+03:00"


def test_generate_synthetic_payloads_preserves_curatable_raw_shape():
    start_at = datetime(2026, 4, 10, tzinfo=timezone.utc)

    payloads = list(
        generate_synthetic_payloads(
            city="varna",
            start_at=start_at,
            days=1,
            interval_minutes=60,
            station_count=2,
        )
    )
    assert len(payloads) == 48
    first_curated = extract_curated_record(payloads[0])
    second_curated = extract_curated_record(payloads[1])
    last_curated = extract_curated_record(payloads[-1])
    assert first_curated["station_id"] == 10001
    assert second_curated["station_id"] == 10002
    assert first_curated["station_name"] == "Synthetic Station 1, Varna"
    assert first_curated["timestamp"] == "2026-04-10T00:00:00+00:00"
    assert last_curated["timestamp"] == "2026-04-10T23:00:00+00:00"
    assert first_curated["latitude"] is not None
    assert first_curated["longitude"] is not None
    assert first_curated["aqi"] is not None
    assert first_curated["dominant_pollutant"] in {"pm10", "no2", "o3"}


def test_generate_synthetic_payloads_rejects_invalid_settings():
    start_at = datetime(2026, 4, 10, tzinfo=timezone.utc)

    try:
        list(
            generate_synthetic_payloads(
                city="varna",
                start_at=start_at,
                days=0,
                interval_minutes=60,
                station_count=1,
            )
        )
    except ValueError as exc:
        assert str(exc) == "days must be greater than zero"
    else:
        raise AssertionError("Expected ValueError")
