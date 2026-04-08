def to_pandas_table(dataframe):
    """Convert a Spark DataFrame to a pandas DataFrame.

    Args:
        dataframe: A Spark DataFrame.

    Returns:
        pandas.DataFrame: A pandas representation of the input table.
    """

    return dataframe.toPandas()
