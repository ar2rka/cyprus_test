import pytest
from task21 import read_db_table, read_parquet
from pyspark.sql.types import *
from pyspark.sql import SparkSession, dataframe

@pytest.mark.usefixtures("spark")
def test_read_db_table(spark: SparkSession):
    jdbc_url = "jdbc:vertica://localhost:5433/notdocker"
    table = "bank.tb_transactions"
    prop = {
        "driver": "com.vertica.jdbc.Driver",
        "user": "dbadmin",
        "password": "foo123"
    }
    mock_input_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', DecimalType(18, 4), True),
        StructField('row_sha2', StringType(), True)
        ])

    df_test = read_db_table(spark, jdbc_url, table, prop)
    assert df_test.schema == mock_input_schema


@pytest.mark.usefixtures("spark")
def test_read_parquet(spark: SparkSession):
    mock_input_schema = StructType([
        StructField('transaction_uid_2', LongType(), True),
        StructField('login_2', LongType(), True),
        StructField('counter_login_2', LongType(), True),
        StructField('transaction_type_2', LongType(), True),
        StructField('unix_timestamp_2', LongType(), True),
        StructField('comment_2', StringType(), True),
        StructField('amount_2', DecimalType(18, 4), True),
        StructField('row2_sha2', StringType(), True)
        ])

    df_test = read_parquet(spark, "../second_source")
    assert df_test.schema == mock_input_schema


