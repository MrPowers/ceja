from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("chispa") \
  .getOrCreate()

import pytest
import ceja.functions as C
from pyspark.sql.functions import col
from chispa.column_comparer import assert_column_equality

def test_nysiis():
    data = [("jellyfish", "JALYF"), ("li", "L"), ("luisa", "LAS")]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_nysiis", C.nysiis(col("word")))
    assert_column_equality(actual_df, "word_nysiis", "expected")

