from math import exp
from random import randint
from datetime import datetime
from pyspark import SparkConf,SparkContext

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(master="local", appName="sparkDemo")

def count_elements(splitIndex, iterator):
    n = sum(1 for _ in iterator)
    yield (splitIndex, n)


def get_part_index(splitIndex, iterator):
    for it in iterator:
        yield (splitIndex, it)


num_parts = 4
# create the large skewed rdd
skewed_large_rdd = sc.parallelize(range(0, num_parts), num_parts).flatMap(lambda x: range(0, int(exp(x))))

print("skewed_large_rdd has %d partitions." % skewed_large_rdd.getNumPartitions())

print(
"The distribution of elements across partitions is: %s"
%str(skewed_large_rdd.mapPartitionsWithIndex(lambda ind, x: count_elements(ind, x)).take(num_parts)))

# put it in (key, value) form
skewed_large_rdd = skewed_large_rdd.mapPartitionsWithIndex(lambda ind, x: get_part_index(ind, x)).cache()
skewed_large_rdd.count()

small_rdd = sc.parallelize(range(0,num_parts), num_parts).map(lambda x: (x, x)).cache()
small_rdd.count()

t0 = datetime.now()
result = skewed_large_rdd.leftOuterJoin(small_rdd)
result.count()
print("The direct join takes %s"%(str(datetime.now() - t0)))