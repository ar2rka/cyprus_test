import pyspark.sql.functions as f
from pyspark.sql import dataframe


def char_rec(df_for_reconciliation: dataframe, tolerance: int) -> dataframe:
    # реконсиляция текста.
    # приводим строки к нижнему регистру -- это первый уровень работы с возможными расхождениями строковых данных;
    # второй уровень -- сравнение расстояния между строками
    # (сколько символов нужно изменить, чтобы из одной строки получить другую);
    # допускаю, что если расстояние = 1, то строки можно считать тождественными;
    # после проверяем на равенство остальных полей;
    # получаем доп строки, прошедшие сверку, которые можно считать валидными
    df_reconciliation_char_input = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2")\
        .filter("comment != '' AND comment_2 != '' AND comment is not null AND comment_2 is not NULL") \
        .filter("""transaction_uid == transaction_uid_2 AND
        NVL(counter_login, 0) == NVL(counter_login_2, 0) AND
        transaction_type == transaction_type_2 AND
        unix_timestamp == unix_timestamp_2 AND
        login == login_2 AND
        amount == amount_2 """)\
        .selectExpr("*", "lower(comment) as l_comment", "lower(comment_2) as l_comment_2") \
        .select("*", f.levenshtein("l_comment", "l_comment_2").alias("levenshtein_distance")) \

    # фильтруем те строки, у которых процент отклонения входит в допущенный диапазон
    df_reconciliation_char = df_reconciliation_char_input \
        .filter(df_reconciliation_char_input["levenshtein_distance"] < tolerance)\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")\

    return df_reconciliation_char


def date_rec(df_for_reconciliation: dataframe, tolerance: int) -> dataframe:
    # Реконсиляция дат.
    # Ранее уже были изменены форматы дат для возможности сравнения, тоже своего рода реконсиляция
    # поэтому здесь второй этап, который актуален, если в данных могут незначитально расходиться ts транзакций.
    # Выбираем строки, которые не равны из-за полей даты, при этом равны по остальным полям
    # Считаем разницу в секундах
    # Делаем допущение, что строки тождественны при расхождении timestamp-ов на 5 минут (300 секунд)
    df_reconciliation_date_input = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2")\
        .filter("unix_timestamp != unix_timestamp_2") \
        .filter("""transaction_uid == transaction_uid_2 AND
        NVL(counter_login, 0) == NVL(counter_login_2, 0) AND
        transaction_type == transaction_type_2 AND
        login == login_2 AND
        NVL(comment, 0) == NVL(comment_2, 0) AND
        amount == amount_2 """)\
        .withColumn("ts_diff", df_for_reconciliation["unix_timestamp"] - df_for_reconciliation["unix_timestamp_2"])

    # фильтруем те строки, у которых процент отклонения входит в допущенный диапазон
    df_reconciliation_date = df_reconciliation_date_input\
        .filter(df_reconciliation_date_input["ts_diff"] < tolerance)\
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")

    return df_reconciliation_date


def int_rec(df_for_reconciliation: dataframe, tolerance: int) -> dataframe:
    # Реконсиляция чисел с допущением
    # фильтруем данные аналогично предыдущим
    # рассчитываем процент отклонения
    df_reconciliation_int2 = df_for_reconciliation \
        .filter("row_sha2 != row2_sha2") \
        .filter(
        """transaction_uid == transaction_uid_2 AND
        NVL(counter_login, 0) == NVL(counter_login_2, 0) AND
        transaction_type == transaction_type_2 AND
        login == login_2 AND
        NVL(comment, 0) == NVL(comment_2, 0) AND
        unix_timestamp == unix_timestamp_2 """)\
        .withColumn("percent_diff", (df_for_reconciliation["amount"] - df_for_reconciliation["amount_2"]) * 100 / df_for_reconciliation["amount"])

    # фильтруем те строки, у которых процент отклонения входит в допущенный диапазон
    df_reconciliation_int_clean = df_reconciliation_int2\
        .filter((df_reconciliation_int2["percent_diff"]) <= tolerance) \
        .filter((df_reconciliation_int2["percent_diff"]) >= tolerance * (-1)) \
        .select("transaction_uid", "login", "counter_login", "transaction_type", "unix_timestamp", "comment", "amount")

    return df_reconciliation_int_clean
