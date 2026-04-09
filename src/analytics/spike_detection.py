from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F


def detect_aqi_spikes(dataframe: DataFrame, aqi_threshold: int, jump_threshold: int) -> DataFrame:
    """Detect AQI spikes by threshold and by sudden increases.

    Args:
        dataframe: The normalized curated records.
        aqi_threshold: A fixed AQI threshold for alert rows.
        jump_threshold: A minimum AQI increase between consecutive rows.

    Returns:
        A Spark dataframe containing the rows that exceed the threshold or sudden-jump rule.
    """
    window_spec = Window.partitionBy("station_id").orderBy("event_timestamp")
    with_previous = dataframe.withColumn("previous_aqi", F.lag("aqi").over(window_spec))
    enriched = (
        with_previous.withColumn("aqi_jump", F.col("aqi") - F.col("previous_aqi"))
        .withColumn("is_threshold_spike", F.col("aqi") > F.lit(aqi_threshold))
        .withColumn("is_jump_spike", F.col("aqi_jump") > F.lit(jump_threshold))
    )
    return enriched.where(F.col("is_threshold_spike") | F.col("is_jump_spike"))
