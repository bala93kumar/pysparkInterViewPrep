from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("sparksql").getOrCreate()
data = spark.read.option("inferSchema","true").csv("file:///D://CODING//pysparkCodes//data//fakefriends.csv").toDF("Id","name","age","numFriends")
data.printSchema()
# data.select("name").show()
# data.filter(" age < 20").show()

# data.groupBy(data.age).count().show()
#
# data.select(data.name,data.age + 10).show()


#task 2 for each age what is the avg numFriends

print("for each age  what is the avg num of friends")

#not working

data.groupBy("age").agg(F.round(F.avg("numFriends").alias("avg_numfriend"),0)).sort("age").show()

# data.select("age", "numFriends").groupBy("age").avg("numFriends").show()

