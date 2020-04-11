from pyspark.sql import SparkSession, dataframe
from pyspark import SparkConf
import pyspark.sql.functions as f
import logging as log
import sys


def daily_agg(df_input: dataframe):
    df_daily_agg = df_input \
        .withColumn("date_from", f.to_date("transaction_ts")) \
        .groupBy("login", "date_from") \
        .sum("sign_amount") \
        .withColumnRenamed("sum(sign_amount)", "daily_sum") \
        .withColumn("calculation_ts", f.current_timestamp()) \

    try:
        df_daily_agg.write.format("jdbc") \
            .option("url", "jdbc:vertica://localhost:5433/notdocker") \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", "bank.tb_daily_agg") \
            .option("user", "dbadmin") \
            .option("password", "foo123") \
            .mode("append") \
            .save()
    except:
        log.error('writing to bank.tb_daily_agg failed')


def monthly_agg(df_input: dataframe):
    df_monthly_agg = df_input \
        .withColumn("date_from", f.date_trunc('mon', "transaction_ts")) \
        .groupBy("login", "date_from") \
        .sum("sign_amount") \
        .withColumnRenamed("sum(sign_amount)", "monthly_sum") \
        .withColumn("calculation_ts", f.current_timestamp())

    try:
        df_monthly_agg.write.format("jdbc") \
            .option("url", "jdbc:vertica://localhost:5433/notdocker") \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", "bank.tb_monthly_agg") \
            .option("user", "dbadmin") \
            .option("password", "foo123") \
            .mode("append") \
            .save()
    except:
        log.error('writing to bank.tb_monthly_agg failed')


def total_agg(df_input: dataframe):
    df_total_agg = df_input \
        .groupBy("login") \
        .sum("sign_amount") \
        .withColumnRenamed("sum(sign_amount)", "total_sum") \
        .withColumn("calculation_ts", f.current_timestamp()) \

    try:
        df_total_agg.write.format("jdbc") \
            .option("url", "jdbc:vertica://localhost:5433/notdocker") \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", "bank.tb_total_agg") \
            .option("user", "dbadmin") \
            .option("password", "foo123") \
            .mode("append") \
            .save()
    except:
        log.error('writing to bank.tb_total_agg failed')


if __name__ == "__main__":
    # подключаем драйвер для Вертики
    conf = SparkConf()
    conf.set("spark.jars", "/Users/burlakaae/gitpersonal/cyprus_test/reconsiliation/vertica-jdbc-9.3.1-0.jar")
    conf.set("spark.executorEnv.PYTHONHASHSEED", "0")

    # инициализируем спарк-сессию
    try:
        spark = SparkSession.builder.master("local[2]").appName("task22").config(conf=conf).getOrCreate()
    except:
        log.error('spark initialization failed')
        sys.exit(1)

    # читаем таблицу с данными после реконсиляции
    try:
        df_source_table = spark.read.format("jdbc") \
            .option("url", "jdbc:vertica://localhost:5433/notdocker") \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", "bank.tb_transactions_clean") \
            .option("user", "dbadmin") \
            .option("password", "foo123") \
            .load()
    except:
        log.error('read from db failed')
        sys.exit(1)

    df_signed = df_source_table \
        .select("login", "transaction_type", "transaction_ts", "amount") \
        .withColumn("sign_amount", df_source_table["amount"]*df_source_table["transaction_type"])\
        .cache()

    # вызов функций со сбором агрегатов
    daily_agg(df_signed)
    monthly_agg(df_signed)
    total_agg(df_signed)
    log.info('great success')
