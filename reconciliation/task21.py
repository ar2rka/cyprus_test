from pyspark.sql import SparkSession, dataframe
from pyspark import SparkConf
import pyspark.sql.functions as f
from reconciliation_by_types import char_rec, date_rec, int_rec
import logging as log
import sys
import configparser


def read_db_table(spark_session: SparkSession, url: str, table: str, properties: dict) -> dataframe:
    # читаем основную таблицу из БД
    try:
        df_source_table = spark_session.read\
            .jdbc(url, table, properties=properties)
    except:
        log.error('failed db connection')
        sys.exit(1)

    # приводим ts к нашему формату для сравнения
    df_source_table_ts = df_source_table \
        .withColumn("unix_timestamp", f.unix_timestamp("transaction_ts")) \
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")

    # добавляем хэш всех соединенных (concat) столбцов
    df_source_table_hash = df_source_table_ts\
        .withColumn("row_sha2", f.sha2(f.concat_ws("||", *df_source_table_ts.columns), 256))

    return df_source_table_hash


def read_parquet(spark_session: SparkSession, source_path: str) -> dataframe:
    # читаем второй источник - файл формата parquet
    try:
        df_second_source = spark_session.read.parquet(source_path)
    except:
        log.error('failed parquet reading')
        sys.exit(1)

    # добавляем колонку с приведенным форматом unix_time к нашему timestamp;
    # удаляем старую колонку с unix_time;
    # делаем select для правильного порядка столбцов, который важен для хэша;
    # добавлем хэш всех соединенных столбцов, как в предыдущем датафрейме;
    df_second_source_ts = df_second_source\
        .withColumnRenamed("epoch_seconds", "unix_timestamp")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")

    # Добавляем постфикс _2 в названия полей, чтоб легче было различать после join-a
    df_second_source_renamed = df_second_source_ts\
        .toDF(*[c + "_2" for c in df_second_source_ts.columns])

    df_second_source_hash = df_second_source_renamed\
        .withColumn("row2_sha2", f.sha2(f.concat_ws("||", *df_second_source_renamed.columns), 256))

    return df_second_source_hash


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    tolerance_vars = config["tolerance_vars"]
    tolerance_percent = tolerance_vars["tolerance_percent"]
    tolerance_levenshtein = tolerance_vars["tolerance_levenshtein"]
    tolerance_seconds = tolerance_vars["tolerance_seconds"]
    jdbc_url = "jdbc:vertica://localhost:5433/notdocker"
    first_source = "bank.tb_transactions"
    second_source = "second_source"
    target_table = "bank.tb_transactions_clean"
    prop = {
        "driver": "com.vertica.jdbc.Driver",
        "user": "dbadmin",
        "password": "foo123"
    }

    # подключаем драйвер для Вертики
    conf = SparkConf()
    conf.set("spark.jars", "vertica-jdbc-9.3.1-0.jar")
    conf.set("spark.executorEnv.PYTHONHASHSEED", "0")

    # инициализируем спарк-сессию
    try:
        spark = SparkSession.builder.master("local[2]").appName("task21").config(conf=conf).getOrCreate()
    except:
        log.error('spark initialization failed')
        sys.exit(1)

    # считываем исходные данные
    df_source_1 = read_db_table(spark, jdbc_url, first_source, prop)
    df_source_2 = read_parquet(spark, second_source)

    # делаем outer join, чтобы остались данные о невалидных транзакциях
    df_for_reconciliation = df_source_1\
        .join(df_source_2,
              df_source_1.transaction_uid == df_source_2.transaction_uid_2,
              "outer")

    # получаем датафрейм с данными, прошедшими сверку
    # .cache() нужен для оптимизации работы спарка: позволяет не повторять переиспользуемые расчеты
    df_clean_data = df_for_reconciliation\
        .filter("row_sha2 == row2_sha2")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")\
        .cache()

    # Реконсиляция по типам
    df_reconciliation_char = char_rec(df_for_reconciliation, tolerance_levenshtein)
    df_reconciliation_date = date_rec(df_for_reconciliation, tolerance_seconds)
    df_reconciliation_int = int_rec(df_for_reconciliation, tolerance_percent)

    # собъединяем датафреймы
    df_final = df_clean_data\
        .union(df_reconciliation_char) \
        .union(df_reconciliation_date) \
        .union(df_reconciliation_int) \
        .withColumn("transaction_ts", f.from_unixtime("unix_timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .drop("unix_timestamp") \
        .select("transaction_uid", "login", "counter_login", "transaction_type", "transaction_ts", "comment", "amount")\

    try:
        # сохраняем (insert) результат в вертику в предварительно созданную таблицу;
        df_final.write\
            .jdbc(jdbc_url, target_table, mode='append', properties=prop)
    except:
        log.error('writing to db failed')
