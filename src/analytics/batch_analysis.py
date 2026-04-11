import logging

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from src.analytics.metrics import (
    compute_average_pollutants,
    compute_daily_aqi,
    compute_aqi_category_distribution,
)
from src.analytics.metrics import compute_average_aqi_by_hour_of_day
from src.analytics.metrics import compute_dominant_pollutant_counts
from src.analytics.metrics import compute_weather_correlations


DEFAULT_HDFS_ROOT = "/data/air-quality"
DEFAULT_CITY = "sofia"

CURATED_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("station_id", IntegerType(), True),
        StructField("station_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("dominant_pollutant", StringType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("o3", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("dew", DoubleType(), True),
    ]
)


def build_curated_input_path(output_root: str, city: str) -> str:
    """Build the curated JSONL input path for a city.

    Args:
        output_root: The HDFS root path used by the pipeline.
        city: The city name used in the storage layout.

    Returns:
        The curated JSONL glob path in HDFS.
    """
    normalized_root = output_root.rstrip("/")
    return f"hdfs://namenode:9000{normalized_root}/{city}/curated/*.jsonl"


def create_spark_session(app_name: str = "air-quality-analytics") -> SparkSession:
    """Create a Spark session for the analytics batch job.

    Args:
        app_name: The application name shown by the analytics job.

    Returns:
        The analytics session used for the batch job.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_curated_dataframe(
    spark: SparkSession, output_root: str, city: str
) -> DataFrame:
    """Load curated AQICN JSONL records from HDFS.

    Args:
        spark: The analytics session used to read curated records.
        output_root: An HDFS root output path.
        city: A city name used in the storage layout.

    Returns:
        A Spark dataframe with the curated records loaded from HDFS.
    """
    input_path = build_curated_input_path(output_root, city)
    return spark.read.schema(CURATED_SCHEMA).json(input_path)


def normalize_curated_dataframe(dataframe: DataFrame) -> DataFrame:
    """Normalize curated AQICN records for analytics.

    Args:
        dataframe: The curated records that include the source timestamp column.

    Returns:
        A Spark dataframe with parsed timestamp, hour, and day columns.
    """
    parsed = dataframe.withColumn(
        "event_timestamp", F.expr("try_to_timestamp(timestamp)")
    )
    filtered = parsed.where(
        F.col("event_timestamp").isNotNull() & F.col("aqi").isNotNull()
    )
    return filtered.withColumn(
        "day", F.date_format(F.col("event_timestamp"), "yyyy-MM-dd")
    ).withColumn("hour", F.hour(F.col("event_timestamp")))


def to_pandas_table(dataframe: DataFrame):
    """Convert analytics results into a notebook-friendly table.

    Args:
        dataframe: The analytics result to convert for notebook display.

    Returns:
        A pandas dataframe for notebook display.
    """
    return dataframe.toPandas()


def run_batch_analysis(
    spark: SparkSession,
    output_root: str,
    city: str,
) -> dict[str, DataFrame]:
    """Run the batch analytics pipeline for a city.

    Args:
        spark: The analytics session used to read curated records and run the reports.
        output_root: An HDFS root output path.
        city: A city name used in the storage layout.

    Returns:
        A dictionary of Spark dataframes keyed by report name.
    """
    logger = logging.getLogger("air_quality.analytics")
    curated = load_curated_dataframe(spark, output_root, city)
    normalized = normalize_curated_dataframe(curated)
    logger.info(
        f"Loaded curated analytics data for {city} from {build_curated_input_path(output_root, city)}"
    )
    return {
        "normalized": normalized,
        "hourly_aqi": compute_average_aqi_by_hour_of_day(normalized),
        "daily_aqi": compute_daily_aqi(normalized),
        "aqi_category_distribution": compute_aqi_category_distribution(normalized),
        "average_pollutants": compute_average_pollutants(normalized),
        "dominant_pollutants": compute_dominant_pollutant_counts(normalized),
        "weather_correlations": compute_weather_correlations(normalized),
    }
