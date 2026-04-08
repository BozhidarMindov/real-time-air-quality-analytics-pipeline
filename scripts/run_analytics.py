import logging
import os

from src.analytics.batch_analysis import DEFAULT_AQI_JUMP_THRESHOLD
from src.analytics.batch_analysis import DEFAULT_AQI_SPIKE_THRESHOLD
from src.analytics.batch_analysis import DEFAULT_CITY
from src.analytics.batch_analysis import DEFAULT_HDFS_ROOT
from src.analytics.batch_analysis import create_spark_session
from src.analytics.batch_analysis import run_batch_analysis


def _getenv_or_default(key: str, default: str | None) -> str | None:
    value = os.getenv(key)
    return value if value else default


def _getenv_int_or_default(key: str, default: int) -> int:
    value = os.getenv(key)
    if not value:
        return default
    return int(value)


def main() -> int:
    """Run the batch analytics job."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    spark = create_spark_session()
    try:
        city = _getenv_or_default("CITY", DEFAULT_CITY)
        output_root = _getenv_or_default("OUTPUT_ROOT", DEFAULT_HDFS_ROOT)
        aqi_threshold = _getenv_int_or_default("AQI_SPIKE_THRESHOLD", DEFAULT_AQI_SPIKE_THRESHOLD)
        jump_threshold = _getenv_int_or_default("AQI_JUMP_THRESHOLD", DEFAULT_AQI_JUMP_THRESHOLD)
        results = run_batch_analysis(
            spark,
            output_root=output_root,
            city=city,
            aqi_threshold=aqi_threshold,
            jump_threshold=jump_threshold,
        )
        logger = logging.getLogger("air_quality.analytics")
        logger.info(f"Computed analytics tables: {', '.join(results)}")
    finally:
        spark.stop()
    return 0


def run_cli() -> int:
    """Run the command-line analytics entrypoint."""

    return main()


if __name__ == "__main__":
    raise SystemExit(run_cli())
