from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import sql

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.jars", "/Users/burlakaae/gitpersonal/cyprus_test/reconsiliation/vertica-jdbc-9.3.1-0.jar")
    conf.set("spark.executorEnv.PYTHONHASHSEED", "0")

    spark = SparkSession.builder.master("local[2]").appName("task21").config(conf=conf).getOrCreate()

    df_source_table = spark.read.format("jdbc")\
        .option("url", "jdbc:vertica://localhost:5433/notdocker")\
        .option("driver", "com.vertica.jdbc.Driver")\
        .option("dbtable", "bank.tb_transactions")\
        .option("user", "dbadmin")\
        .option("password", "foo123")\
        .load()

    df_changed = df_source_table.filter(Col('transaction_uid' == 3000000).show()


