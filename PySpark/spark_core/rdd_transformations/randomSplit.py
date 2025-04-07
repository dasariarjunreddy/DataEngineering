# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "randomSplit")

#########################################
#   randomSplit - Example 1
#########################################
rdd_data = sc.parallelize(range(1,50)) 

rdd_splits = rdd_data.randomSplit([0.5, 0.5],20)
rdd1 = rdd_splits[0]
rdd2 = rdd_splits[1]
#rdd3 = rdd_splits[2]
print(rdd1.collect())
print(rdd2.collect())
#print(rdd3.collect())
"""rdd1_o = rdd_splits[0]
rdd1_o = rdd1.collect()
#print(rdd1_o)
rdd2.collect()
rdd2_o = rdd2.collect()
#print(rdd2_o)
rdd3.collect()
rdd3_o = rdd3.collect()
print(rdd2_o)
print(rdd3_o)"""
sc.stop()