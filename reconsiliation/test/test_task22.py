import pytest
from task22 import daily_agg, monthly_agg, total_agg
from pyspark.sql.types import *
from pyspark.sql import SparkSession, dataframe

@pytest.mark.usefixtures("spark")
def test_daily_agg(spark: SparkSession, df_input: dataframe):
    mock_input_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('transaction_type', IntegerType(), True),
        StructField('transaction_ts', TimestampType(), True),
        StructField('amount', DecimalType(), True)
        ])
    df_input_data = spark.createDataFrame([
            ("10001001", "1", "2020-04-09 20:28:42", "130"),
            ("10001001", "1", "2020-04-09 20:28:42", "120"),
        ], schema=mock_input_schema
    )

    mock_result_schema = StructType([
        StructField('login', IntegerType(), True),
        StructField('date_from', DateType(), True),
        StructField('daily_sum', DecimalType(), True)
        ])
    df_mock_result = spark.createDataFrame([
            ("10001001", "2020-04-09", "130"),
        ], schema=mock_result_schema
    )
    print('testtt --------')
    df_input_data.show()


    # df_test_daily_agg = daily_agg(df_mock_data)
