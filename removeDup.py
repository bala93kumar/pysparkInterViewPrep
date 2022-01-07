from pyspark.sql import SparkSession
from pyspark.sql import Row

from pyspark.sql import functions as F

from pyspark.sql.window import Window

spark = SparkSession.builder.appName("removeDup").getOrCreate()

data = spark.read.option("header","true").option("inferSchema","true").csv("file:///D://CODING//pysparkCodes//data//duplicate.csv")

data.show()

print("grouped count")
data.groupBy("Name", "Age","Education", "Year").count().where("count > 1").show()

data_1 =  data.groupBy("Name", "Age","Education", "Year").count().where("count > 1")

win = Window.partitionBy("Name").orderBy(F.col("Year").desc())

data_2 = data.withColumn("rank", F.row_number().over(win)).filter("rank == 1")

data_2.show()
print(data.rdd.getNumPartitions())






