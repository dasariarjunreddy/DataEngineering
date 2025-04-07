# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "map")


rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 4), ("b", 2),("a", 1), ("b", 5), ("a", 1), ("b", 7)])
rdd2 = rdd.groupByKey()

out1 = sorted(rdd.groupByKey(2).mapValues(len).collect())
print(out1)

out2 = sorted(rdd.groupByKey().mapValues(list).collect())
print(out2)

sc.stop()


