import pytest
from reconciliation_by_types import char_rec, int_rec, date_rec
from pyspark.sql.types import *
from pyspark.sql import SparkSession, dataframe


@pytest.mark.usefixtures("spark")
def test_char_rec(spark: SparkSession):
    tolerance = 1
    mock_input_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('row_sha2', StringType(), True),
        StructField('transaction_uid_2', LongType(), True),
        StructField('login_2', LongType(), True),
        StructField('counter_login_2', LongType(), True),
        StructField('transaction_type_2', LongType(), True),
        StructField('unix_timestamp_2', LongType(), True),
        StructField('comment_2', StringType(), True),
        StructField('amount_2', IntegerType(), True),
        StructField('row2_sha2', StringType(), True)
        ])
    df_input_data = spark.createDataFrame([
            (3000005, 10005001, None, -1, 1586453322, 'Rental Pay', 50, 'mock_hash',
             3000005, 10005001, None, -1, 1586453322, 'rental pay', 50, 'mock_hash2',
             )
        ], schema=mock_input_schema
    )

    expected_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', IntegerType(), True)
        ])
    df_expected_result = spark.createDataFrame([
            (3000005, 10005001, None, -1, 1586453322, 'Rental Pay', 50)
        ], schema=expected_schema
    )

    df_test = char_rec(df_input_data, tolerance)
    assert df_expected_result.collect() == df_test.collect()


@pytest.mark.usefixtures("spark")
def test_date_rec(spark: SparkSession):
    tolerance = 300
    mock_input_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('row_sha2', StringType(), True),
        StructField('transaction_uid_2', LongType(), True),
        StructField('login_2', LongType(), True),
        StructField('counter_login_2', LongType(), True),
        StructField('transaction_type_2', LongType(), True),
        StructField('unix_timestamp_2', LongType(), True),
        StructField('comment_2', StringType(), True),
        StructField('amount_2', IntegerType(), True),
        StructField('row2_sha2', StringType(), True)
        ])
    df_input_data = spark.createDataFrame([
            (3000005, 10005001, None, -1, 1586453320, 'Rental Pay', 50, 'mock_hash',
             3000005, 10005001, None, -1, 1586453322, 'Rental Pay', 50, 'mock_hash2',
             )
        ], schema=mock_input_schema
    )

    expected_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', IntegerType(), True)
        ])
    df_expected_result = spark.createDataFrame([
            (3000005, 10005001, None, -1, 1586453320, 'Rental Pay', 50)
        ], schema=expected_schema
    )

    df_test = date_rec(df_input_data, tolerance)
    assert df_expected_result.collect() == df_test.collect()


@pytest.mark.usefixtures("spark")
def test_int_rec(spark: SparkSession):
    tolerance = 5
    mock_input_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('row_sha2', StringType(), True),
        StructField('transaction_uid_2', LongType(), True),
        StructField('login_2', LongType(), True),
        StructField('counter_login_2', LongType(), True),
        StructField('transaction_type_2', LongType(), True),
        StructField('unix_timestamp_2', LongType(), True),
        StructField('comment_2', StringType(), True),
        StructField('amount_2', IntegerType(), True),
        StructField('row2_sha2', StringType(), True)
        ])
    df_input_data = spark.createDataFrame([
            (3000005, 10005001, None, -1, 1586453322, 'Rental Pay', 50, 'mock_hash',
             3000005, 10005001, None, -1, 1586453322, 'Rental Pay', 51, 'mock_hash2',
             )
        ], schema=mock_input_schema
    )

    expected_schema = StructType([
        StructField('transaction_uid', LongType(), True),
        StructField('login', LongType(), True),
        StructField('counter_login', LongType(), True),
        StructField('transaction_type', LongType(), True),
        StructField('unix_timestamp', LongType(), True),
        StructField('comment', StringType(), True),
        StructField('amount', IntegerType(), True)
        ])
    df_expected_result = spark.createDataFrame([
            (3000005, 10005001, None, -1, 1586453322, 'Rental Pay', 50)
        ], schema=expected_schema
    )

    df_test = int_rec(df_input_data, tolerance)
    assert df_expected_result.collect() == df_test.collect()
