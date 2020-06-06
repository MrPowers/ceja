from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col

import jellyfish as J

@udf(returnType=StringType())
def nysiis(s):
     return None if s == None else J.nysiis(s)

@udf(returnType=StringType())
def metaphone(s):
     return None if s == None else J.metaphone(s)


