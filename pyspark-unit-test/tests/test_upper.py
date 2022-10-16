from pyspark.sql import SparkSession, DataFrame

import pytest
from pyspark_unit_test import job


# Get one spark session for the whole test session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.getOrCreate()
    return spark 


def test_uppercase(spark):
    
    # given 
    columns = ["name", "age", "zip"]
    data = [["walid", 30, 75015], ["julie", 28, 75019]]
    df = spark.createDataFrame(data, columns)

    # when
    actual = job.to_uppercase(df, ["name"])

    # then 
    expected_data = [["WALID", 30, 75015], ["JULIE", 28, 75019]]
    expected = spark.createDataFrame(expected_data, columns)

    rows = actual.collect()
    expected_rows = expected.collect()

    # Compare dataframes collects
    assert rows == expected_rows
