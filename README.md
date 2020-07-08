# ceja

PySpark phonetic, stemming, and string matching algorithms.  Use the power of PySpark to run these algos on massive datasets!

## Installation and basic usage

Run `pip install ceja` to install the library.

Import the functions with `import ceja`.  After importing the code you can run functions like `ceja.nysiis`, `ceja.jaro_winkler_similarity`, etc.

## Public interface summary

* Phonetic algorithms
  * nysiis
  * metaphone
  * match_rating_codex
* Stemming
  * porter_stem
* String similarity
  * damerau_levenshtein_distance
  * hamming_distance
  * jaro_similarity
  * jaro_winkler_similarity
  * match_rating_comparison

## Phonetic algorithms

### NYSIIS

```python
data = [
    ("jellyfish",),
    ("li",),
    ("luisa",),
    (None,)
]
df = spark.createDataFrame(data, ["word"])
actual_df = df.withColumn("word_nysiis", ceja.nysiis(col("word")))
actual_df.show()
```

```
+---------+-----------+
|     word|word_nysiis|
+---------+-----------+
|jellyfish|      JALYF|
|       li|          L|
|    luisa|        LAS|
|     null|       null|
+---------+-----------+
```

### Metaphone

```python
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
actual_df.show()
```

```
+---------+--------------+
|     word|word_metaphone|
+---------+--------------+
|jellyfish|          JLFX|
|       li|             L|
|    luisa|            LS|
|   Klumpz|         KLMPS|
|   Clumps|         KLMPS|
|     null|          null|
+---------+--------------+
```

### Match rating codex

```python
data = [
    ("jellyfish",),
    ("li",),
    ("luisa",),
    (None,)
]
df = spark.createDataFrame(data, ["word"])
actual_df = df.withColumn("word_match_rating_codex", ceja.match_rating_codex(col("word")))
actual_df.show()
```

```
+---------+-----------------------+
|     word|word_match_rating_codex|
+---------+-----------------------+
|jellyfish|                 JLYFSH|
|       li|                      L|
|    luisa|                     LS|
|     null|                   null|
+---------+-----------------------+
```

## Stemming algorithms

### Porter stem

```python
data = [
    ("chocolates",),
    ("chocolatey",),
    ("choco",),
    (None,)
]
df = spark.createDataFrame(data, ["word"])
actual_df = df.withColumn("word_porter_stem", ceja.porter_stem(col("word")))
actual_df.show()
```

```
+----------+----------------+
|      word|word_porter_stem|
+----------+----------------+
|chocolates|          chocol|
|chocolatey|      chocolatei|
|     choco|           choco|
|      null|            null|
+----------+----------------+
```

## Similarity algorithms

### Damerau Levenshtein Distance

```python
data = [
    ("jellyfish", "smellyfish"),
    ("li", "lee"),
    ("luisa", "bruna"),
    (None, None)
]
df = spark.createDataFrame(data, ["word1", "word2"])
actual_df = df.withColumn("damerau_levenshtein_distance", ceja.damerau_levenshtein_distance(col("word1"), col("word2")))
actual_df.show()
```

```
+---------+----------+----------------------------+
|    word1|     word2|damerau_levenshtein_distance|
+---------+----------+----------------------------+
|jellyfish|smellyfish|                           2|
|       li|       lee|                           2|
|    luisa|     bruna|                           4|
|     null|      null|                        null|
+---------+----------+----------------------------+
```

## Hamming distance

```python
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
```

```
+---------+----------+----------------+
|    word1|     word2|hamming_distance|
+---------+----------+----------------+
|jellyfish|smellyfish|               9|
|       li|       lee|               2|
|    luisa|     bruna|               4|
|     null|      null|            null|
+---------+----------+----------------+
```

### Jaro similarity

```python
data = [
    ("jellyfish", "smellyfish"),
    ("li", "lee"),
    ("luisa", "bruna"),
    ("hi", "colombia"),
    (None, None)
]
df = spark.createDataFrame(data, ["word1", "word2"])
actual_df = df.withColumn("jaro_similarity", ceja.jaro_similarity(col("word1"), col("word2")))
actual_df.show()
```

```
+---------+----------+---------------+
|    word1|     word2|jaro_similarity|
+---------+----------+---------------+
|jellyfish|smellyfish|      0.8962963|
|       li|       lee|      0.6111111|
|    luisa|     bruna|            0.6|
|       hi|  colombia|            0.0|
|     null|      null|           null|
+---------+----------+---------------+
```

### Jaro Winkler similarity

```python
data = [
    ("jellyfish", "smellyfish"),
    ("li", "lee"),
    ("luisa", "bruna"),
    (None, None)
]
df = spark.createDataFrame(data, ["word1", "word2"])
actual_df = df.withColumn("jaro_winkler_similarity", ceja.jaro_winkler_similarity(col("word1"), col("word2")))
actual_df.show()
```

```
+---------+----------+-----------------------+
|    word1|     word2|jaro_winkler_similarity|
+---------+----------+-----------------------+
|jellyfish|smellyfish|              0.8962963|
|       li|       lee|              0.6111111|
|    luisa|     bruna|                    0.6|
|     null|      null|                   null|
+---------+----------+-----------------------+
```

### Match rating comparison

```python
data = [
    ("mat", "matt"),
    ("there", "their"),
    ("luisa", "bruna"),
    (None, None)
]
df = spark.createDataFrame(data, ["word1", "word2"])
actual_df = df.withColumn("match_rating_comparison", ceja.match_rating_comparison(col("word1"), col("word2")))
actual_df.show()
```

```
+-----+-----+-----------------------+
|word1|word2|match_rating_comparison|
+-----+-----+-----------------------+
|  mat| matt|                   true|
|there|their|                   true|
|luisa|bruna|                  false|
| null| null|                   null|
+-----+-----+-----------------------+
```

## Contributing

Contributions are welcome and encouraged.  Feel free to open issues or send pull requests.

If you make a lot of good contributions, you'll be granted push access to the repo.

The best contributions to make would be implementing these functions as Spark native functions.

