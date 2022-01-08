from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

from pyspark.sql.types import  StructType ,StructField , StringType , IntegerType , FloatType


spark = SparkSession.builder.appName("sparksql").getOrCreate()

schema = StructType([StructField("cust_id", IntegerType(), True), \
                     StructField("item_id", IntegerType(), True), \
                     StructField("amount_spend", FloatType(), True)])

data = spark.read.schema(schema).csv("file:///D://CODING//pysparkCodes//data//customer-orders.csv")

data.show()

data_1 = data.groupBy("cust_id").agg(F.round(F.sum("amount_spend")).alias("sum")).orderBy(F.col("sum").desc())

data_1.show()
