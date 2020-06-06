from pyspark.sql import SparkSession


spark = SparkSession.builder \
  .master("local") \
  .appName("chispa") \
  .getOrCreate()


import pytest
from pyspark.sql.functions import col
import ceja.functions as C
from chispa.column_comparer import assert_column_equality


def test_nysiis():
    data = [("jellyfish", "JALYF"), ("li", "L"), ("luisa", "LAS"), (None, None)]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_nysiis", C.nysiis(col("word")))
    assert_column_equality(actual_df, "word_nysiis", "expected")


def test_metaphone():
    data = [("jellyfish", "JLFX"), ("li", "L"), ("luisa", "LS"), (None, None)]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_metaphone", C.metaphone(col("word")))
    assert_column_equality(actual_df, "word_metaphone", "expected")


def test_jaro_distance():
    data = [("jellyfish", "smellyfish", "JLFX"), ("li", "lee", "L"), ("luisa", "bruna", "LS"), (None, None, None)]
    df = spark.createDataFrame(data, ["word1", "word2", "expected"])
    actual_df = df.withColumn("word_jaro_distance", C.jaro_distance(col("word1"), col("word2")))
    actual_df.show()
    # assert_column_equality(actual_df, "word_jaro_distance", "expected")
