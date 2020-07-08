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
        ("jellyfish", "JALYF"),
        ("li", "L"),
        ("luisa", "LAS"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_nysiis", ceja.nysiis(col("word")))
    assert_column_equality(actual_df, "word_nysiis", "expected")


def test_metaphone():
    data = [
        ("jellyfish", "JLFX"),
        ("li", "L"),
        ("luisa", "LS"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_metaphone", ceja.metaphone(col("word")))
    assert_column_equality(actual_df, "word_metaphone", "expected")


def test_match_rating_codex():
    data = [
        ("jellyfish", "JLYFSH"),
        ("li", "L"),
        ("luisa", "LS"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_match_rating_codex", ceja.match_rating_codex(col("word")))
    assert_column_equality(actual_df, "word_match_rating_codex", "expected")


def test_porter_stem():
    data = [
        ("chocolates", "chocol"),
        ("chocolatey", "chocolatei"),
        ("choco", "choco"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["word", "expected"])
    actual_df = df.withColumn("word_porter_stem", ceja.porter_stem(col("word")))
    assert_column_equality(actual_df, "word_porter_stem", "expected")


def test_damerau_levenshtein_distance():
    data = [
        ("jellyfish", "smellyfish", 2),
        ("li", "lee", 2),
        ("luisa", "bruna", 4),
        (None, None, None)
    ]
    df = spark.create_df(
        data,
        [("word1", StringType(), True), ("word2", StringType(), True), ("expected", IntegerType(), True)]
    )
    actual_df = df.withColumn("word_damerau_levenshtein_distance", ceja.damerau_levenshtein_distance(col("word1"), col("word2")))
    assert_column_equality(actual_df, "word_damerau_levenshtein_distance", "expected")


def test_hamming_distance():
    data = [
        ("jellyfish", "smellyfish", 9),
        ("li", "lee", 2),
        ("luisa", "bruna", 4),
        (None, None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2", "expected"])
    actual_df = df.withColumn("word_hamming_distance", ceja.hamming_distance(col("word1"), col("word2")))
    assert_column_equality(actual_df, "word_hamming_distance", "expected")


def test_jaro_similarity():
    data = [
        ("jellyfish", "smellyfish", 0.89),
        ("li", "lee", 0.61),
        ("luisa", "bruna", 0.6),
        ("hi", "colombia", 0.0),
        (None, None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2", "expected"])
    actual_df = df.withColumn("word_jaro_distance", ceja.jaro_similarity(col("word1"), col("word2")))
    assert_approx_column_equality(actual_df, "word_jaro_distance", "expected", 0.01)


def test_jaro_winkler_similarity():
    data = [
        ("jellyfish", "smellyfish", 0.89),
        ("li", "lee", 0.61),
        ("luisa", "bruna", 0.6),
        (None, None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2", "expected"])
    actual_df = df.withColumn("word_jaro_winkler_similarity", ceja.jaro_winkler_similarity(col("word1"), col("word2")))
    assert_approx_column_equality(actual_df, "word_jaro_winkler_similarity", "expected", 0.01)


def test_match_rating_comparison():
    data = [
        ("mat", "matt", True),
        ("there", "their", True),
        ("luisa", "bruna", False),
        (None, None, None)
    ]
    df = spark.createDataFrame(data, ["word1", "word2", "expected"])
    actual_df = df.withColumn("word_match_rating_comparison", ceja.match_rating_comparison(col("word1"), col("word2")))
    assert_column_equality(actual_df, "word_match_rating_comparison", "expected")

