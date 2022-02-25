from pyspark.sql import SparkSession
from pyspark.sql import functions as f


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("file:///D://CODING//pandySpark//Spark-Programming-In-Python-master//Spark-Programming-In-Python-master//AggDemo//data//invoices.csv")



    invoice_df.show(10,False)

    # grouped  = invoice_df.select(f.col("InvoiceNo")).agg(f.sum("Quantity").alias("TotalQuantity"), f.  )

     #FIRST REQUIREMENT

    # grouped = invoice_df.groupBy("InvoiceNo", "Country"). \
    #     agg(f.sum(f.col("Quantity") ).alias("TotalQuantity") , \
    #         f.round(f.sum( f.expr( ("Quantity * UnitPrice")) ) ,2) )
    #
    # grouped.show(10,False)

    #second requirement

    InvoiceValue = f.round( f.sum(f.expr("Quantity* UnitPrice") ),2).alias("InvoiceValue")

    exSummary_df = invoice_df.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")).\
        where("year(InvoiceDate) == 2010"). \
        withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))).\
        groupBy("Country","WeekNumber").\
        agg( f.countDistinct("InvoiceNo").alias("NumInvoices") , f.sum("Quantity").alias("TotalQuantity") , InvoiceValue )


    exSummary_df.show()




