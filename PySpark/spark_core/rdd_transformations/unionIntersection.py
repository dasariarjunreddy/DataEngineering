# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "Union")

rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9])
rdd2 = sc.parallelize([5,6,7,8,9,10,11,12,13,14])
rdd3 = sc.parallelize({1:"Sunday",2:"Monday",3:"Tuesday"})
# Union
union = rdd1.union(rdd2).collect()
print(union)

union1 = rdd1.union(rdd3).collect()
print(union1)  

#intersection
intersection = rdd1.intersection(rdd2).collect()
print(intersection) 
sc.stop()