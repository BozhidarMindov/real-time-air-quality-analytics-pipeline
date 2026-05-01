import logging
import os

from src.analytics.batch_analysis import DEFAULT_HDFS_ROOT
from src.analytics.batch_analysis import create_spark_session
from src.analytics.batch_analysis import run_batch_analysis
from src.common.config import get_required_env
from src.common.logging import configure_logging


def main() -> int:
    """Run the batch analytics job.

    Returns:
        The process exit code.
    """
    configure_logging()
    city = get_required_env("CITY")
    output_root = os.getenv("OUTPUT_ROOT") or DEFAULT_HDFS_ROOT
    spark = create_spark_session()
    try:
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
