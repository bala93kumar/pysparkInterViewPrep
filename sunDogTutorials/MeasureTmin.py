from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import  StructType , StructField , StringType , IntegerType , FloatType

spark = SparkSession.builder.appName("MinTemp").getOrCreate()
schema = StructType([StructField("stationId",StringType(),True), StructField("data",IntegerType(),True),
                     StructField("measureType",StringType(),True), StructField("temp",FloatType(),True)])

data = spark.read.schema(schema).csv("file:///D://CODING//pysparkCodes//data//1800.csv")
data.show()
data_1 = data.filter(F.col("measureType") == "TMIN")
data_1.show()

minTempByStId =  data_1.groupBy("stationId").agg(F.min("temp").alias("minTemp"))
minTempByStId.show()

minTempByStId_1 = minTempByStId.withColumn("temp",F.round( F.col("minTemp") * 0.1 * (9.0/5.0) + 32 ,2) ).\
    withColumn("sumed", F.col("minTemp") + F.col("temp")).orderBy(F.col("temp").desc())

minTempByStId_1.show()

results = minTempByStId_1.collect()

for i in results :
    print(i[0] + "\t{}F".format(i[2]))
