from pyspark.sql.types import StringType, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import udf
import jellyfish as J
import stringzilla as sz


# PHONETIC ALGORITHMS


@udf(returnType=StringType())
def nysiis(s):
    """Applies the NYSIIS phonetic algorithm to a string."""
    return None if s == None else J.nysiis(s)


@udf(returnType=StringType())
def metaphone(s):
    """Applies the Metaphone phonetic algorithm to a string."""
    return None if s == None else J.metaphone(s)


@udf(returnType=StringType())
def match_rating_codex(s):
    """Generates the Match Rating codex for a string."""
    return None if s == None else J.match_rating_codex(s)


# STEMMING ALGORITHMS


@udf(returnType=StringType())
def porter_stem(s):
    """Applies the Porter stemming algorithm to a string."""
    return None if s == None else J.porter_stem(s)


# STRING COMPARISON


@udf(returnType=IntegerType())
def levenshtein_distance(s1, s2):
    """Calculates the Levenshtein distance between two strings or byte-arrays."""
    if s1 == None or s2 == None:
        return None

    if isinstance(s1, str) and isinstance(s2, str):
        return sz.edit_distance_unicode(s1, s2)
    else:
        return sz.edit_distance(s1, s2)


@udf(returnType=IntegerType())
def damerau_levenshtein_distance(s1, s2):
    """Calculates the Damerau-Levenshtein distance between two strings."""
    return None if s1 == None or s2 == None else J.damerau_levenshtein_distance(s1, s2)


@udf(returnType=IntegerType())
def hamming_distance(s1, s2):
    """Calculates the Hamming distance between two strings or byte-arrays."""
    if s1 == None or s2 == None:
        return None

    if isinstance(s1, str) and isinstance(s2, str):
        return sz.hamming_distance_unicode(s1, s2)
    else:
        return sz.hamming_distance(s1, s2)


@udf(returnType=FloatType())
def jaro_similarity(s1, s2):
    """Calculates the Jaro similarity between two strings."""
    return None if s1 == None or s2 == None else J.jaro_similarity(s1, s2)


@udf(returnType=FloatType())
def jaro_winkler_similarity(s1, s2):
    """Calculates the Jaro-Winkler similarity between two strings."""
    return None if s1 == None or s2 == None else J.jaro_winkler_similarity(s1, s2)


@udf(returnType=BooleanType())
def match_rating_comparison(s1, s2):
    """Determines if two strings are considered equivalent under the Match Rating Approach."""
    return None if s1 == None or s2 == None else J.match_rating_comparison(s1, s2)
