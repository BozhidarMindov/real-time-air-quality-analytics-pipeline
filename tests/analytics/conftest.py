import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """A local Spark session for analytics tests."""

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("air-quality-analytics-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield spark
    spark.stop()
