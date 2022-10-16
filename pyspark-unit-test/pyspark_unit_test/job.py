from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List

spark = SparkSession.builder.appName("spark-unit-test").getOrCreate()


def to_uppercase(df: DataFrame, columns_to_transform: List) -> DataFrame:
    """Uppercase the columns provided in the dataframe
    Args:
        df (DataFrame): Input Dataframe
        columns_to_transform (List): List of columns to uppercase
    Returns:
        DataFrame: The transformed DataFrame
    """

    # Loop through columns to transform and convert to uppercase
    for column in columns_to_transform:
        if column in df.columns:
            df = df.withColumn(column, F.upper(F.col(column)))

    return df


def main():
    # Read the data
    df = spark.read.csv("./pyspark_unit_test/resources/client_bdd.csv", header=True)

    # Apply transformations
    df = to_uppercase(df, ["name"])

    # Write the data
    df.write.csv("./data/output/client.csv")
