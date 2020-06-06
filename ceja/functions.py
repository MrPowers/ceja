from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col

import jellyfish as J

@udf(returnType=StringType())
def nysiis(s):
     return J.nysiis(s)

