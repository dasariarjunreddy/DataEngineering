# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "takeSample")

rdd = sc.parallelize(range(0, 10))

#countByKey : Count the number of elements for each key, and return the result as a dictionary.

rdd = sc.parallelize([("python", 1), ("pyspark", 1), ("spark", 1), ("hadoop", 1), ("python", 1), ("pyspark", 1), ("spark", 1)])
out1 = sorted(rdd.countByKey().items())
print(out1)


sc.stop()

