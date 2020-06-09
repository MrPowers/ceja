from pyspark.sql.types import StringType, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import udf, col
import jellyfish as J


# PHONETIC ALGORITHMS

@udf(returnType=StringType())
def nysiis(s):
     return None if s == None else J.nysiis(s)


@udf(returnType=StringType())
def metaphone(s):
     return None if s == None else J.metaphone(s)


@udf(returnType=StringType())
def match_rating_codex(s):
     return None if s == None else J.match_rating_codex(s)


# STEMMING ALGORITHMS

@udf(returnType=StringType())
def porter_stem(s):
     return None if s == None else J.porter_stem(s)


# STRING COMPARISON

@udf(returnType=IntegerType())
def damerau_levenshtein_distance(s1, s2):
     return None if s1 == None or s2 == None else J.damerau_levenshtein_distance(s1, s2)


@udf(returnType=IntegerType())
def hamming_distance(s1, s2):
     return None if s1 == None or s2 == None else J.hamming_distance(s1, s2)


@udf(returnType=FloatType())
def jaro_similarity(s1, s2):
     return None if s1 == None or s2 == None else J.jaro_similarity(s1, s2)


@udf(returnType=FloatType())
def jaro_winkler_similarity(s1, s2):
     return None if s1 == None or s2 == None else J.jaro_winkler_similarity(s1, s2)


@udf(returnType=BooleanType())
def match_rating_comparison(s1, s2):
     return None if s1 == None or s2 == None else J.match_rating_comparison(s1, s2)

