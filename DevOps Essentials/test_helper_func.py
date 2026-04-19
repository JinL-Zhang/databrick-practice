import pytest
import helper_func
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType
from pyspark.sql.functions import col, when
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual

# Setup a pytest fixture to create a SparkSession for testing
@pytest.fixture(scope="session")
def spark():
    # Create a SparkSession for testing, if one already exists, reuse
    spark = SparkSession.builder.getOrCreate()
    # pass the spark session to the test function
    yield spark

def test_get_health_csv_schema():
    # defin the expected schema that the helper function should return
    expected_schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("PII", StringType(), True),
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True),
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True)
    ])

    # call the helper function to get the schema
    actual_schema = helper_func.get_health_csv_schema()

    # compare the expected and actual schema
    assertSchemaEqual(expected_schema, actual_schema)



def test_high_cholest_map(spark):
  # create the sample dataframe to test
  df = spark.createDataFrame(
      [(0,),
        (1,),
        (2,),
        (3,),
        (4,),
        (None,)],
      ['value']
  )

  # apply the func on the sample data
  actual_df = df.withColumn("actual", helper_func.high_cholest_map("value"))

  # create the static expected dataframe
  expected_df = spark.createDataFrame(
    [
      (0,'Normal'),
      (1,'Above Average'),
      (2,'High'),
      (3,'Unknown'),
      (4,'Unknown'),
      (None, 'Unknown')
    ],
    schema = StructType([
      StructField("value", LongType(), True),
      StructField("actual", StringType(), True)
    ])
  )

  # assert the actual dataframe matches the expected dataframe
  assertDataFrameEqual(actual_df, expected_df)
  print("Test Pass")