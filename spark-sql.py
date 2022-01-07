from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("sparksql").getOrCreate()

#lines = spark.sparkContext.textFile("file:///D://CODING//pysparkCodes//data//fakefriends.csv")
# schemaPeople = spark.createDataFrame(lines)
# schemaPeople.createOrReplaceTempView("people")

data = spark.read.option("inferSchema","true").csv("file:///D://CODING//pysparkCodes//data//fakefriends.csv").toDF("Id","name","age","numFriends")

# all_data = spark.sql("select * from people ")
data.printSchema()
data.show()

schemaPeople = data.groupBy("age").count().orderBy("age")
schemaPeople.show()

filtered = schemaPeople.filter(schemaPeople["age"] > 20)
filtered.show()


