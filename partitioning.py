from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F


import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql.types import  StructType ,StructField , StringType , IntegerType , FloatType

spark = SparkSession.builder.appName("sparksql").getOrCreate()

list1 = [(1),(2),(3)]
rdd = spark.sparkContext.parallelize(list1)
row_rdd = rdd.map(lambda x: Row(x))

schema = StructType([
    StructField("values", IntegerType(), True)
])
df = spark.createDataFrame(row_rdd,schema=schema)
df.explain()
df.show()

input("press enter")

