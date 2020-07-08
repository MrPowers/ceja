from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .master("local") \
    .appName("ceja") \
    .getOrCreate()


from pyspark.sql.types import *
from pyspark.sql import SparkSession


def create_df(self, rows_data, col_specs):
    struct_fields = list(map(lambda x: StructField(*x), col_specs))
    return self.createDataFrame(data=rows_data, schema=StructType(struct_fields))


SparkSession.create_df = create_df


import pytest
from pyspark.sql.functions import col

import ceja
from chispa.column_comparer import assert_column_equality, assert_approx_column_equality


def test_nysiis():
    data = [
        ("jellyfish",),
        ("li",),
        ("luisa",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["word"])
    actual_df = df.withColumn("word_nysiis", ceja.nysiis(col("word")))
    print("\nNYSIIS")
    actual_df.show()


def test_metaphone():
    data = [
        ("jellyfish",),
        ("li",),
        ("luisa",),
        ("Klumpz",),
        ("Clumps",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["word"])
    actual_df = df.withColumn("word_metaphone", ceja.metaphone(col("word")))
    print("\nMetaphone")
    actual_df.show()


def test_match_rating_codex():
    data = [
        ("jellyfish",),
        ("li",),
        ("luisa",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["word"])
    actual_df = df.withColumn("word_match_rating_codex", ceja.match_rating_codex(col("word")))
    print("\nMatch rating codex")
    actual_df.show()


def test_porter_stem():
    data = [
        ("chocolates",),
        ("chocolatey",),
        ("choco",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["word"])
    actual_df = df.withColumn("word_porter_stem", ceja.porter_stem(col("word")))
    print("\nPorter stem")
    actual_df.show()


def test_damerau_levenshtein_distance():
    data = [
        ("jellyfish", "smellyfish"),
        ("li", "lee"),
        ("luisa", "bruna"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2"])
    actual_df = df.withColumn("damerau_levenshtein_distance", ceja.damerau_levenshtein_distance(col("word1"), col("word2")))
    print("\nDamerau Levenshtein distance")
    actual_df.show()


def test_hamming_distance():
    data = [
        ("jellyfish", "smellyfish"),
        ("li", "lee"),
        ("luisa", "bruna"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2"])
    actual_df = df.withColumn("hamming_distance", ceja.hamming_distance(col("word1"), col("word2")))
    print("\nHamming distance")
    actual_df.show()


def test_jaro_similarity():
    data = [
        ("jellyfish", "smellyfish"),
        ("li", "lee"),
        ("luisa", "bruna"),
        ("hi", "colombia"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2"])
    actual_df = df.withColumn("jaro_similarity", ceja.jaro_similarity(col("word1"), col("word2")))
    print("\nJaro similarity")
    actual_df.show()


def test_jaro_winkler_similarity():
    data = [
        ("jellyfish", "smellyfish"),
        ("li", "lee"),
        ("luisa", "bruna"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2"])
    actual_df = df.withColumn("jaro_winkler_similarity", ceja.jaro_winkler_similarity(col("word1"), col("word2")))
    print("\nJaro Winkler similarity")
    actual_df.show()


def test_match_rating_comparison():
    data = [
        ("mat", "matt"),
        ("there", "their"),
        ("luisa", "bruna"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2"])
    actual_df = df.withColumn("match_rating_comparison", ceja.match_rating_comparison(col("word1"), col("word2")))
    print("\nMatch rating comparison")
    actual_df.show()


