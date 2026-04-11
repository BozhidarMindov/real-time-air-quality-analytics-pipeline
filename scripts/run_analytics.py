import logging

from src.analytics.batch_analysis import DEFAULT_CITY
from src.analytics.batch_analysis import DEFAULT_HDFS_ROOT
from src.analytics.batch_analysis import create_spark_session
from src.analytics.batch_analysis import run_batch_analysis
from src.common.env import get_env_or_default
from src.common.logging import configure_logging


def main() -> int:
    """Run the batch analytics job.

    Returns:
        The process exit code.
    """
    configure_logging()
    spark = create_spark_session()
    try:
        city = get_env_or_default("CITY", DEFAULT_CITY)
        output_root = get_env_or_default("OUTPUT_ROOT", DEFAULT_HDFS_ROOT)
        results = run_batch_analysis(
            spark,
            output_root=output_root,
            city=city,
        )
        logger = logging.getLogger("air_quality.analytics")
        logger.info(f"Computed analytics tables: {', '.join(results)}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
