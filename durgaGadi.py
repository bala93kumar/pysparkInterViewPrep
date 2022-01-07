from pyspark import  SparkConf , SparkContext


import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# spark = SparkSession.builder.getOrCreate()


sc = SparkContext(master="local", appName="sparkDemo")

print(sc.textFile("file:///C://deckofcards.txt").first())