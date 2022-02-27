from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import  Window

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    agg_op = spark.read.parquet("file:///D://CODING//pysparkCodes//output//AggDemoOp")

    # agg_op.show()

    # agg_op.orderBy("Country","WeekNumber").show()

    window_total =    Window.partitionBy(f.col("Country")).orderBy(f.col("WeekNumber").desc()).\
        rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary = agg_op.withColumn("RunningTotal", f.sum("InvoiceValue").over(window_total))

    summary.show()


