import json
from pathlib import Path


NON_POLLUTANT_IAQI_KEYS = {"dew", "h", "p", "t", "w"}


def get_nested(mapping: dict | None, *keys: str):
    """Return a nested mapping value when every key exists.

    Args:
        mapping: A mapping that may contain nested values.
        *keys: A sequence of nested keys.

    Returns:
        The nested value when every key exists, or `None` otherwise.
    """
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_pollutants(data: dict) -> dict[str, float]:
    """Extract numeric pollutant values from an AQICN `iaqi` mapping.

    Args:
        data: A raw AQICN data payload.

    Returns:
        Pollutant values keyed by AQICN pollutant name.
    """
    iaqi = data.get("iaqi")
    if not isinstance(iaqi, dict):
        return {}

    pollutants = {}
    for key in sorted(iaqi):
        if key in NON_POLLUTANT_IAQI_KEYS:
            continue
        value = get_nested(iaqi, key, "v")
        if isinstance(value, (int, float)):
            pollutants[key] = float(value)
    return pollutants


def extract_curated_record(payload: dict) -> dict:
    """Extract the curated fields used by the analytics pipeline.

    Args:
        payload: A raw AQICN payload.

    Returns:
        The curated record with the required analytics fields.
    """
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}

    return {
        "timestamp": get_nested(data, "time", "iso"),
        "station_id": data.get("idx"),
        "station_name": get_nested(data, "city", "name"),
        "latitude": get_geo_value(data, 0),
        "longitude": get_geo_value(data, 1),
        "aqi": data.get("aqi"),
        "dominant_pollutant": data.get("dominentpol"),
        "pollutants": extract_pollutants(data),
        "temperature": get_nested(data, "iaqi", "t", "v"),
        "humidity": get_nested(data, "iaqi", "h", "v"),
        "wind": get_nested(data, "iaqi", "w", "v"),
        "pressure": get_nested(data, "iaqi", "p", "v"),
        "dew": get_nested(data, "iaqi", "dew", "v"),
    }


def extract_day(payload: dict, processing_date: str) -> str:
    """Extract the day partition from a payload or fall back to the processing date.

    Args:
        payload: A raw AQICN payload.
        processing_date: A fallback processing date.

    Returns:
        The day string in `YYYY-MM-DD` format.
    """
    timestamp = get_nested(payload, "data", "time", "iso")
    if isinstance(timestamp, str) and len(timestamp) >= 10:
        return timestamp[:10]
    return processing_date


def load_curated_observation_cache(path: Path) -> dict[str, str]:
    """Load the persisted curated observation cache when present.

    Args:
        path: A local cache file path.

    Returns:
        The persisted station-to-timestamp cache.
    """
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def persist_curated_observation_cache(path: Path, cache: dict[str, str]) -> None:
    """Persist the curated observation cache with an atomic replace.

    Args:
        path: A local cache file path.
        cache: The station-to-timestamp cache to persist.

    Returns:
        None.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(cache, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(path)


def build_curated_record_key(
    curated_record: dict,
    logger=None,
) -> tuple[str, str] | None:
    """Return the deduplication key for a curated record when complete.

    Args:
        curated_record: A curated AQICN record.
        logger: An optional logger used for skip messages.

    Returns:
        The station-and-timestamp deduplication key, or `None` when required fields are missing.
    """
    station_id = curated_record.get("station_id")
    timestamp = curated_record.get("timestamp")
    if station_id is None or timestamp is None:
        if logger is not None:
            logger.warning("Skipping curated record without station_id or timestamp")
        return None
    return str(station_id), timestamp


def filter_curated_records(
    curated_records: list[dict],
    cached_timestamps: dict[str, str],
    logger=None,
) -> tuple[list[dict], dict[str, str]]:
    """Drop cached and duplicate curated records and return the updated cache.

    Args:
        curated_records: The curated records for the current write batch.
        cached_timestamps: The persisted station-to-timestamp cache.
        logger: An optional logger used for skip messages.

    Returns:
        The curated records to write and the updated cache state.
    """
    updated_cache = dict(cached_timestamps)
    filtered_records: list[dict] = []

    for record in curated_records:
        key = build_curated_record_key(record, logger=logger)
        if key is None:
            continue

        station_id, timestamp = key
        if updated_cache.get(station_id) == timestamp:
            if logger is not None:
                logger.info(
                    f"Skipping duplicate curated record for station_id={station_id} timestamp={timestamp}"
                )
            continue

        filtered_records.append(record)
        updated_cache[station_id] = timestamp

    return filtered_records, updated_cache


def get_geo_value(data: dict, index: int):
    """Return a latitude or longitude value from the AQICN geo array.

    Args:
        data: A raw AQICN data payload.
        index: A zero-based geo array index.

    Returns:
        The geo value at the requested position, or `None` when it is missing.
    """
    geo = get_nested(data, "city", "geo")
    if isinstance(geo, list) and len(geo) > index:
        return geo[index]
    return None
