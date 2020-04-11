import pytest
from task22 import daily_agg, monthly_agg, total_agg
from pyspark.sql.types import *
from pyspark.sql import SparkSession, dataframe
import pyspark.sql.functions as f

@pytest.mark.usefixtures("spark")
def test_daily_agg(spark: SparkSession):
    mock_input_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('transaction_type', IntegerType(), True),
        StructField('transaction_ts', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('sign_amount', IntegerType(), True)
        ])
    df_input_data = spark.createDataFrame([
            (10001001, 1, "2020-04-09 20:28:42", 130, 130),
            (10001001, 1, "2020-04-09 20:28:42", 120, 120)
        ], schema=mock_input_schema
    )

    mock_result_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('date_from', StringType(), True),
        StructField('daily_sum', IntegerType(), True)
        ])
    df_mock_result = spark.createDataFrame([
            (10001001, "2020-04-09", 250)
        ], schema=mock_result_schema
    ).select("login", f.to_date("date_from", 'yyyy-MM-dd'), "daily_sum")\

    df_test_daily_agg = daily_agg(df_input_data)

    assert df_mock_result.collect() == df_test_daily_agg.select("login", "date_from", "daily_sum").collect()


@pytest.mark.usefixtures("spark")
def test_monthly_agg(spark: SparkSession):
    mock_input_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('transaction_type', IntegerType(), True),
        StructField('transaction_ts', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('sign_amount', IntegerType(), True)
        ])
    df_input_data = spark.createDataFrame([
            (10001001, 1, "2020-04-09 20:28:42", 130, 130),
            (10001001, 1, "2020-04-10 20:28:42", 120, 120)
        ], schema=mock_input_schema
    )

    mock_result_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('date_from', StringType(), True),
        StructField('daily_sum', IntegerType(), True)
        ])
    df_mock_result = spark.createDataFrame([
            (10001001, "2020-04-09", 250)
        ], schema=mock_result_schema
    ).select("login", f.to_date("date_from", 'yyyy-MM-dd'), "monthly_sum")\

    df_test_monthly_agg = monthly_agg(df_input_data)

    assert df_mock_result.collect() == df_test_monthly_agg.select("login", "date_from", "monthly_sum").collect()


@pytest.mark.usefixtures("spark")
def test_total_agg(spark: SparkSession):
    mock_input_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('transaction_type', IntegerType(), True),
        StructField('transaction_ts', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('sign_amount', IntegerType(), True)
        ])
    df_input_data = spark.createDataFrame([
            (10001001, 1, "2020-05-09 20:28:42", 130, 130),
            (10001001, 1, "2020-04-10 20:28:42", 120, 120)
        ], schema=mock_input_schema
    )

    mock_result_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('total_sum', IntegerType(), True)
        ])
    df_mock_result = spark.createDataFrame([
            (10001001, 250)
        ], schema=mock_result_schema
    ).select("login", "total_sum")\

    df_test_total_agg = total_agg(df_input_data)

    assert df_mock_result.collect() == df_test_total_agg.select("login", "total_sum").collect()
