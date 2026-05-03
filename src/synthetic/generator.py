import copy
import json
import math
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Iterator


DEFAULT_EXAMPLE_RAW_PATH = (
    Path(__file__).resolve().parent / "examples" / "raw" / "2026-05-02.jsonl"
)


def load_example_payload(path: str | Path = DEFAULT_EXAMPLE_RAW_PATH) -> dict:
    """Load the first raw AQICN example payload from a JSONL file.

    Args:
        path: The JSONL file path containing raw AQICN example records.

    Returns:
        The first decoded raw AQICN payload.

    Raises:
        ValueError: The file does not contain a JSON record.
    """
    with Path(path).open(encoding="utf-8-sig") as file:
        for line in file:
            if line.strip():
                return json.loads(line)
    raise ValueError(f"{path} does not contain an example payload")


def build_synthetic_payload(
    base_payload: dict,
    city: str,
    timestamp: datetime,
    station_index: int,
    interval_index: int,
) -> dict:
    """Build one synthetic AQICN-shaped raw payload.

    Args:
        base_payload: A raw AQICN example payload used as the schema template.
        city: The city label used in the synthetic station name.
        timestamp: The observation timestamp.
        station_index: The zero-based synthetic station index.
        interval_index: The zero-based generated interval index.

    Returns:
        A synthetic raw AQICN payload.
    """
    payload = copy.deepcopy(base_payload)
    data = payload.setdefault("data", {})
    station_number = station_index + 1
    profile_shift = station_index * 4
    hour = timestamp.hour + timestamp.minute / 60
    daily_wave = math.sin(((hour - 7) / 24) * 2 * math.pi)
    commute_wave = math.sin(((hour - 17) / 24) * 2 * math.pi)
    slow_variation = math.sin((interval_index + station_index) / 12)

    pm10 = round(18 + profile_shift + daily_wave * 6 + slow_variation * 3, 1)
    no2 = round(12 + profile_shift + commute_wave * 5 + slow_variation * 2, 1)
    o3 = round(32 + daily_wave * -8 + station_index * 2, 1)
    temperature = round(15 + daily_wave * 9, 1)
    humidity = round(60 - daily_wave * 16, 1)
    wind = round(2.5 + abs(slow_variation) * 2, 1)
    pressure = round(1015 + slow_variation * 5, 1)
    dew = round(temperature - ((100 - humidity) / 5), 1)
    pollutant_values = {"pm10": pm10, "no2": no2, "o3": o3}
    dominant_pollutant = max(pollutant_values, key=pollutant_values.get)
    aqi = max(1, round(20 + pm10 * 0.9 + no2 * 0.5 + o3 * 0.4))

    latitude = 42.6977 + station_index * 0.012
    longitude = 23.3219 + station_index * 0.014
    data["idx"] = 10000 + station_number
    data["aqi"] = aqi
    data["dominentpol"] = dominant_pollutant
    data["city"] = {
        "geo": [round(latitude, 6), round(longitude, 6)],
        "name": f"Synthetic Station {station_number}, {city.title()}",
        "url": f"https://synthetic.local/{city.lower()}/station-{station_number}",
        "location": "",
    }
    data["iaqi"] = {
        "dew": {"v": dew},
        "h": {"v": humidity},
        "no2": {"v": no2},
        "o3": {"v": o3},
        "p": {"v": pressure},
        "pm10": {"v": pm10},
        "t": {"v": temperature},
        "w": {"v": wind},
    }
    data["time"] = {
        "s": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "tz": timestamp.strftime("%z") or "+00:00",
        "v": int(timestamp.timestamp()),
        "iso": timestamp.isoformat(),
    }
    return payload


def generate_synthetic_payloads(
    city: str,
    start_at: datetime,
    days: int = 5,
    interval_minutes: int = 5,
    station_count: int = 1,
    example_payload: dict | None = None,
) -> Iterator[dict]:
    """Generate raw AQICN-shaped payloads for synthetic Kafka loading.

    Args:
        city: The city label used in synthetic station names.
        start_at: The timestamp for the first generated interval.
        days: The number of full days to generate.
        interval_minutes: The number of minutes between generated observations.
        station_count: The number of synthetic stations per interval.
        example_payload: An optional raw AQICN payload used as the base shape.

    Returns:
        An iterator of raw AQICN-shaped payloads.

    Raises:
        ValueError: One of the generation settings is not positive.
    """
    if days <= 0:
        raise ValueError("days must be greater than zero")
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be greater than zero")
    if station_count <= 0:
        raise ValueError("station_count must be greater than zero")

    base_payload = example_payload or load_example_payload()
    interval_count = days * 24 * 60 // interval_minutes

    for interval_index in range(interval_count):
        timestamp = start_at + timedelta(minutes=interval_index * interval_minutes)
        for station_index in range(station_count):
            yield build_synthetic_payload(
                base_payload=base_payload,
                city=city,
                timestamp=timestamp,
                station_index=station_index,
                interval_index=interval_index,
            )
