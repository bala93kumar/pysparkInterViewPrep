from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("sparksql").getOrCreate()
data = spark.read.text("file:///D://CODING//pysparkCodes//data//book.txt")

data.show(truncate=False)
data_1 = data.select(F.explode(F.split(F.col("value"),"\\W+")).alias("word"))
data_1.show(truncate=False)
data_2 = data_1.select(F.lower(F.col("word")).alias("word")).groupBy("word").agg(F.count("word").alias("counts")).sort("counts")
data_2.show(data_2.count())