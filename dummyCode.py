# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create a spark session
spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()

# Create an empty RDD
emp_RDD = spark.sparkContext.emptyRDD()

# Create empty schema
columns = StructType([])

# Create an empty RDD with empty schema
data = spark.createDataFrame(data=emp_RDD,
                             schema=columns)

# Print the dataframe
print('Dataframe :')
data.show()

# Print the schema
print('Schema :')
data.printSchema()