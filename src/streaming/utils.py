import json
from pathlib import Path


def normalize_hdfs_path(path: Path | str) -> str:
    """Normalize a local or HDFS path into an absolute HDFS path string.

    Args:
        path: A local-style or HDFS-style path value.

    Returns:
        The absolute HDFS path without a trailing slash.
    """
    value = path.as_posix() if isinstance(path, Path) else str(path)
    value = value.strip().replace("\\", "/")
    if value.startswith("hdfs://"):
        suffix = value.split("://", 1)[1]
        slash_index = suffix.find("/")
        value = suffix[slash_index:] if slash_index >= 0 else "/"
    value = value.rstrip("/")
    if not value.startswith("/"):
        value = f"/{value}"
    return value or "/"


def join_hdfs_root(root: Path | str, *parts: str) -> str:
    """Join an HDFS root path with normalized child parts.

    Args:
        root: An HDFS root path.
        *parts: Additional HDFS path parts.

    Returns:
        The normalized HDFS path.
    """
    base = normalize_hdfs_path(root)
    suffix = "/".join(part.strip("/") for part in parts if part)
    return f"{base}/{suffix}" if suffix else base


def build_daily_output_path(
    output_root: Path | str, city: str, record_type: str, day: str
) -> str:
    """Build the daily output path for a raw or curated JSONL file.

    Args:
        output_root: An HDFS root output path.
        city: A city name used in the storage layout.
        record_type: A record type directory such as `raw` or `curated`.
        day: A day string in `YYYY-MM-DD` format.

    Returns:
        The HDFS path for the daily JSONL file.
    """
    return join_hdfs_root(output_root, city, record_type, f"{day}.jsonl")


def serialize_jsonl(records: list[dict]) -> str:
    """Serialize a list of dictionaries as compact JSON Lines.

    Args:
        records: The records to serialize.

    Returns:
        The compact JSON Lines payload.
    """
    return "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
