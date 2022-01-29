import sys

from pyspark.sql import SparkSession

from pyspark.sql import  functions as F

from lib.logger import  Log4j

if __name__ == "__main__":

    spark = SparkSession \
            .builder\
        .master("local[3]")\
        .appName("HelloSpark").getOrCreate()

    survey_df = spark.read \
        .option("header","true")\
        .option("inferSchema","true")\
    .csv("file:///D://CODING//pysparkCodes//data//sample.csv")

    # print(survey_df.rdd.getNumPartitions())

    #job 2

    spark.conf.set("spark.sql.shuffle.partitions",2)


    partioned = survey_df.repartition(3)

    # count_df = partioned.filter(F. col("Age") < 40) \
    # .select("Age","Gender","Country","state")\
    # .groupBy("Country")\
    # .count()
    #

    # partioned.write.option("header", "true").option("inferSchema", "true").csv(
    #     "file:///D://CODING//pysparkCodes//output//helloSparkSql_3_repartition_op")

    # survey_df.show()


    # second code to check and repartition  by a col

    partioned.repartition(F.col("Country")).write.option("header", "true").option("inferSchema", "true").csv(
        "file:///D://CODING//pysparkCodes//output//helloSparkSql_4_repartition_col")


    input("Press Enter")
