from pyspark.sql import SparkSession, dataframe
from pyspark import SparkConf
import pyspark.sql.functions as f
import logging as log
import sys


def daily_agg(df_input: dataframe) -> dataframe:
    return df_input.withColumn("date_from", f.to_date("transaction_ts")) \
        .groupBy("login", "date_from") \
        .sum("sign_amount") \
        .withColumnRenamed("sum(sign_amount)", "daily_sum") \
        .withColumn("calculation_ts", f.current_timestamp())


def monthly_agg(df_input: dataframe) -> dataframe:
    return df_input.withColumn("date_from", f.date_trunc('mon', "transaction_ts")) \
        .groupBy("login", "date_from") \
        .sum("sign_amount") \
        .withColumnRenamed("sum(sign_amount)", "monthly_sum") \
        .withColumn("calculation_ts", f.current_timestamp())


def total_agg(df_input: dataframe) -> dataframe:
    return df_input.groupBy("login") \
        .sum("sign_amount") \
        .withColumnRenamed("sum(sign_amount)", "total_sum") \
        .withColumn("calculation_ts", f.current_timestamp())


if __name__ == "__main__":
    # подключаем драйвер для Вертики
    conf = SparkConf()
    conf.set("spark.jars", "vertica-jdbc-9.3.1-0.jar")
    conf.set("spark.executorEnv.PYTHONHASHSEED", "0")
    jdbc_url = "jdbc:vertica://localhost:5433/notdocker"
    prop = {
        "driver": "com.vertica.jdbc.Driver",
        "user": "dbadmin",
        "password": "foo123"
    }

    # инициализируем спарк-сессию
    try:
        spark = SparkSession.builder.master("local[2]").appName("task22").config(conf=conf).getOrCreate()
    except:
        log.error('spark initialization failed')
        sys.exit(1)

    # читаем таблицу с данными после реконсиляции
    try:
        df_source_table = spark.read \
            .jdbc(jdbc_url, "bank.tb_transactions_clean", properties=prop)
    except:
        log.error('read from db failed')
        sys.exit(1)

    df_signed = df_source_table \
        .select("login", "transaction_type", "transaction_ts", "amount") \
        .withColumn("sign_amount", df_source_table["amount"]*df_source_table["transaction_type"])\
        .cache()

    # вызов функций со сбором агрегатов и запись результатов в бд
    df_daily_agg = daily_agg(df_signed)
    try:
        df_daily_agg.write \
            .jdbc(jdbc_url, "bank.tb_daily_agg", mode='append', properties=prop)
    except:
        log.error('writing to bank.tb_daily_agg failed')

    df_monthly_agg = monthly_agg(df_signed)
    try:
        df_monthly_agg.write \
            .jdbc(jdbc_url, "bank.tb_monthly_agg", mode='append', properties=prop)
    except:
        log.error('writing to bank.tb_monthly_agg failed')

    df_total_agg = total_agg(df_signed)
    try:
        df_total_agg.write.write \
            .jdbc(jdbc_url, "bank.tb_total_agg", mode='append', properties=prop)
    except:
        log.error('writing to bank.tb_total_agg failed')

    log.info('great success')
