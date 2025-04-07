# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "flatmap")

#########################################
#   flatMap Vs map - Example 1
#########################################
rdd_data = sc.parallelize([1,2,3]) 

rdd_map1 = rdd_data.map(lambda x: [x, x, x])
rdd_flatmap1 = rdd_data.flatMap(lambda x: [x, x, x])
rdd_flatmap = rdd_data.flatMap(lambda x: [x, x+1, x+2])
#rdd_flatmap = rdd_data.flatMap(lambda x: [x,x,x])
#for x in rdd_flatmap_1.collect(): print(x)
print(rdd_map1.collect(),sep="|")
print(rdd_flatmap1.collect())
print(rdd_flatmap.collect())

rdd_map = rdd_data.flatMap(lambda x: [x,x+1,x+2])
print(rdd_map.collect())

