from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _safe_corr(left_column: str, right_column: str):
    """Return a correlation aggregate that yields null for degenerate series."""

    return F.expr(
        "try_divide("
        f"covar_samp({left_column}, {right_column}), "
        f"stddev_samp({left_column}) * stddev_samp({right_column})"
        ")"
    )


def compute_hourly_aqi(dataframe: DataFrame) -> DataFrame:
    """Compute the average AQI for each hour bucket.

    Args:
        dataframe: A normalized curated Spark DataFrame.

    Returns:
        DataFrame: A DataFrame ordered by hour with average AQI values.
    """

    return (
        dataframe.where(F.col("hour").isNotNull())
        .groupBy("hour")
        .agg(F.avg("aqi").alias("avg_aqi"))
        .orderBy("hour")
    )


def compute_average_pollutants(dataframe: DataFrame) -> DataFrame:
    """Compute the average pollutant values used in the report.

    Args:
        dataframe: A normalized curated Spark DataFrame.

    Returns:
        DataFrame: A one-row DataFrame with pollutant averages.
    """

    return dataframe.agg(
        F.avg("pm10").alias("avg_pm10"),
        F.avg("no2").alias("avg_no2"),
        F.avg("o3").alias("avg_o3"),
    )


def compute_dominant_pollutant_counts(dataframe: DataFrame) -> DataFrame:
    """Count dominant pollutant values ordered by frequency.

    Args:
        dataframe: A normalized curated Spark DataFrame.

    Returns:
        DataFrame: A DataFrame with dominant pollutant counts.
    """

    return (
        dataframe.where(F.col("dominant_pollutant").isNotNull())
        .groupBy("dominant_pollutant")
        .count()
        .orderBy(F.desc("count"), F.asc("dominant_pollutant"))
    )


def compute_weather_correlations(dataframe: DataFrame) -> DataFrame:
    """Compute weather correlations against AQI.

    Args:
        dataframe: A normalized curated Spark DataFrame.

    Returns:
        DataFrame: A one-row DataFrame with AQI correlation values.
    """

    return dataframe.agg(
        _safe_corr("aqi", "temperature").alias("aqi_temperature_corr"),
        _safe_corr("aqi", "humidity").alias("aqi_humidity_corr"),
        _safe_corr("aqi", "wind").alias("aqi_wind_corr"),
    )
