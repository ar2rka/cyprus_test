from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import sql
import pyspark.sql.functions as f

if __name__ == "__main__":

    # подключаем драйвер для Вертики
    conf = SparkConf()
    conf.set("spark.jars", "/Users/burlakaae/gitpersonal/cyprus_test/reconsiliation/vertica-jdbc-9.3.1-0.jar")
    conf.set("spark.executorEnv.PYTHONHASHSEED", "0")

    # инициализируем спарк-сессию
    spark = SparkSession.builder.master("local[2]").appName("task21").config(conf=conf).getOrCreate()

    # читаем основную таблицу из БД
    df_source_table = spark.read.format("jdbc")\
        .option("url", "jdbc:vertica://localhost:5433/notdocker")\
        .option("driver", "com.vertica.jdbc.Driver")\
        .option("dbtable", "bank.tb_transactions")\
        .option("user", "dbadmin")\
        .option("password", "foo123")\
        .load()

    # приводим ts к нашему формату для сравнения
    df_second_source_ts = df_source_table\
        .withColumn("transaction_ts2", f.to_timestamp("transaction_ts", "yyyy-MM-dd HH:mm:ss"))\
        .drop("transaction_ts")\
        .withColumnRenamed("transaction_ts2", "transaction_ts")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "transaction_ts", "comment", "amount")

    # добавляем хэш всех соединенных (concat) столбцов
    df_source_table_hash = df_second_source_ts\
        .withColumn("row_sha2", f.sha2(f.concat_ws("||", *df_second_source_ts.columns), 256))

    # читаем второй источник - файл формата parquet
    df_second_source = spark.read.parquet("second_source")

    # добавляем колонку с приведенным форматом unix_time к нашему timestamp;
    # удаляем старую колонку с unix_time;
    # делаем select для правильного порядка столбцов, который важен для хэша;
    # добавлем хэш всех соединенных столбцов, как в предыдущем датафрейме;
    df_second_source_ts = df_second_source\
        .withColumn("transaction_ts", f.from_unixtime("epoch_seconds", "yyyy-MM-dd HH:mm:ss"))\
        .drop("epoch_seconds")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "transaction_ts", "comment", "amount")

    # Добавляем постфикс _2 в названия полей, чтоб легче было различать после join-a
    df_second_source_renamed = df_second_source_ts\
        .toDF(*[c + "_2" for c in df_second_source_ts.columns])

    df_second_source_hash = df_second_source_renamed\
        .withColumn("row2_sha2", f.sha2(f.concat_ws("||", *df_second_source_renamed.columns), 256))

    # делаем outer join, чтобы остались данные о невалидных транзакциях
    df_for_reconciliation = df_source_table_hash\
        .join(df_second_source_hash,
              df_source_table_hash.transaction_uid == df_second_source_hash.transaction_uid_2,
              "outer")

    # получаем датафрейм с данными, прошедшими сверку
    df_clean_data = df_for_reconciliation\
        .filter("row_sha2 == row2_sha2")

    # приводим строки к нижнему регистру;
    # сравнием расстояние между строками (сколько символов нужно изменить, чтобы из одной строки получить другую)
    # допускаю, что если расстояние = 1, то строки можно считать тождественными;
    # получаем доп строки, прошедшие сверку, которые можно считать валидными
    df_reconciliation_char = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2")\
        .filter("comment != '' AND comment_2 != '' AND comment is not null AND comment_2 is not NULL")\
        .selectExpr("*", "lower(comment) as l_comment", "lower(comment_2) as l_comment_2") \
        .select("*", f.levenshtein("l_comment", "l_comment_2").alias("levenshtein_distance"))\
        .filter("levenshtein_distance < 2")\



    # df_changes = spark.read.format("jdbc")\
    #     .option("url", "jdbc:vertica://localhost:5433/notdocker")\
    #     .option("driver", "com.vertica.jdbc.Driver")\
    #     .option("dbtable", "bank.tb_transactions2")\
    #     .option("user", "dbadmin")\
    #     .option("password", "foo123")\
    #
    # df_second_source.write.csv("second_source.csv")

    # df_changed = df_source_table.filter("transaction_uid < 3000090").union(df_changes)
    #
    # df_changed\
    #     .withColumn("epoch_seconds", f.unix_timestamp("transaction_ts"))\
    #     .drop("transaction_ts")\
    #     .select("transaction_uid", "login", "counter_login", "transaction_type", "epoch_seconds", "comment", "amount")\
    #     .orderBy("transaction_uid", ascending=False) \
    #     .repartition(1).write.mode("overwrite").parquet("second_source")



