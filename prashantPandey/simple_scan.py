from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
import sys
import os
import sys
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("sparksql").getOrCreate()

log4jLogger = spark._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

# spark.conf.set("spark.sql.shuffle.partitions", 2)


# logger = log4j(spark)


data = spark.read.option("inferSchema","true").option("header","true").csv("file:///D://CODING//pysparkCodes//data//fakefriends.csv").toDF("Id","name","age","numFriends")

data_partition = data.repartition(2)
count_df = data_partition.filter(F.col("age") < 20 ).select("name","age").groupBy("age").count()
# collected = count_df.collect()

count_df.show()

data.coalesce(1).write.option("header","true").option("inferSchema","true").csv("file:///D://CODING//pysparkCodes//output//simple_scan")

# count_df.write.csv("file:///D://CODING//pysparkCodes//output//simple_scan")

# for i in collected:
#     print(i[0])

# print(data.rdd.getNumPartitions())
# input("enter")