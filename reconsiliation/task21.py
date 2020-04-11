from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import sql
import pyspark.sql.functions as f

percent = 5

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
        .withColumn("unix_timestamp", f.unix_timestamp("transaction_ts")) \
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")
    # .withColumn("transaction_ts2", f.to_timestamp("transaction_ts", "yyyy-MM-dd HH:mm:ss")) \
    # .drop("transaction_ts")\
    # .withColumnRenamed("transaction_ts2", "transaction_ts")\

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
        .withColumnRenamed("epoch_seconds", "unix_timestamp")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")
# .withColumn("transaction_ts", f.from_unixtime("epoch_seconds", "yyyy-MM-dd HH:mm:ss")) \
#     .drop("epoch_seconds") \

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
        .filter("row_sha2 == row2_sha2")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")\

    # реконсиляция текста
    # приводим строки к нижнему регистру;
    # сравнием расстояние между строками (сколько символов нужно изменить, чтобы из одной строки получить другую)
    # допускаю, что если расстояние = 1, то строки можно считать тождественными;
    # после проверяем на равенство остальных полей
    # получаем доп строки, прошедшие сверку, которые можно считать валидными
    df_reconciliation_char = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2")\
        .filter("comment != '' AND comment_2 != '' AND comment is not null AND comment_2 is not NULL")\
        .selectExpr("*", "lower(comment) as l_comment", "lower(comment_2) as l_comment_2") \
        .select("*", f.levenshtein("l_comment", "l_comment_2").alias("levenshtein_distance"))\
        .filter("levenshtein_distance < 2")\
        .filter(
            """transaction_uid == transaction_uid_2 AND
            NVL(counter_login, 0) == NVL(counter_login_2, 0) AND
            transaction_type == transaction_type_2 AND
            unix_timestamp == unix_timestamp_2 AND
            login == login_2 AND
            amount == amount_2 """)\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")\

    # Реконсиляция дат.
    # Ранее уже были изменены форматы дат для возможности сравнения, тоже своего рода реконсиляция
    # поэтому здесь второй этап, который актуален, если в данных могут незначитально расходиться ts транзакций.
    # Выбираем строки, которые не равны из-за полей даты, при этом равны по остальным полям
    # Считаем разницу в секундах
    # Делаем допущение, что строки тождественны при расхождении timestamp-ов на 5 минут (300 секунд)
    df_reconciliation_date = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2")\
        .filter("unix_timestamp != unix_timestamp_2") \
        .filter(
        """transaction_uid == transaction_uid_2 AND
        NVL(counter_login, 0) == NVL(counter_login_2, 0) AND
        transaction_type == transaction_type_2 AND
        login == login_2 AND
        NVL(comment, 0) == NVL(comment_2, 0) AND
        amount == amount_2 """)\
        .withColumn("ts_diff", df_for_reconciliation["unix_timestamp"] - df_for_reconciliation["unix_timestamp_2"])\
        .filter("ts_diff < 300")\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")\

    # Реконсиляция чисел с допущением
    # фильтруем данные аналогично предыдущим
    # рассчитываем процент отклонения
    df_reconciliation_int = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2") \
        .filter(
        """transaction_uid == transaction_uid_2 AND
        NVL(counter_login, 0) == NVL(counter_login_2, 0) AND
        transaction_type == transaction_type_2 AND
        login == login_2 AND
        NVL(comment, 0) == NVL(comment_2, 0) AND
        unix_timestamp == unix_timestamp_2 """)\
        .withColumn("percent_diff", (df_for_reconciliation["amount"] - df_for_reconciliation["amount_2"]) * 100 / df_for_reconciliation["amount"])\

    # фильтруем те строки, у которых процент отклонения входит в допущенный диапазон
    df_reconciliation_int_clean = df_reconciliation_int\
        .filter((df_reconciliation_int["percent_diff"]) <= percent) \
        .filter((df_reconciliation_int["percent_diff"]) >= percent * (-1)) \
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")

    # собъединяем датафреймы
    df_final = df_clean_data\
        .union(df_reconciliation_char) \
        .union(df_reconciliation_date) \
        .union(df_reconciliation_int_clean) \
        .withColumn("transaction_ts", f.from_unixtime("unix_timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .drop("unix_timestamp") \
        .select("transaction_uid", "login", "counter_login", "transaction_type", "transaction_ts", "comment", "amount")


    # сохраняем результат в вертику в предварительно созданную таблицу;
    # результаты добавляю к существующим, а не перезаписываю
    df_final.write.format("jdbc")\
        .option("url", "jdbc:vertica://localhost:5433/notdocker")\
        .option("driver", "com.vertica.jdbc.Driver")\
        .option("dbtable", "bank.tb_transactions_clean")\
        .option("user", "dbadmin")\
        .option("password", "foo123") \
        .mode("append") \
        .save()






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



