from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import collections
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# spark = SparkSession.builder.getOrCreate()

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")

sc = SparkContext(conf=conf)

lines = sc.textFile("file:///C://Users//balak//Downloads//Compressed//ml-100k//u.data")
ratings = lines.map(lambda x : x.split()[2])
results = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(results.items()))

for key, value in sortedResults.iteritems():
    print(key, value)



