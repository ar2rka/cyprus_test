import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    conf = SparkConf()
    conf.set("spark.jars", "../vertica-jdbc-9.3.1-0.jar")
    conf.set("spark.executorEnv.PYTHONHASHSEED", "0")
    spark = SparkSession.builder.master("local[2]").appName("test").config(conf=conf).getOrCreate()
    return spark
