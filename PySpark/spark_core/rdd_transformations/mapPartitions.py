# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "mapPartitons")

#########################################
#   mapPartitions - Example 1
#########################################
rdd_parallal = sc.parallelize(range(1, 12),3)  #(1,2,3), (4,5,6,7), (8,9,10,11)

rdd = sc.parallelize([1, 2, 3, 4], 2) # [1,2] , [3,4]
def f(iterator): yield sum(iterator)
rdd.mapPartitions(f).collect()


print( rdd_parallal.getNumPartitions() )
def total(iterator): 
    yield sum(iterator)

#rdd_map = rdd_parallal.map(total)
#print(rdd_map.collect())
rdd_mp = rdd_parallal.mapPartitions(total)
print(rdd_mp.collect())


#########################################
#   mapPartitions - Example 2
#########################################
def show(iterator): 
    yield list(iterator)

rdd_mp2 = rdd_parallal.mapPartitions(show)
print(rdd_mp2.collect())

# example 3
rdd_parallal_2 = sc.parallelize(range(1, 12),3)

print( rdd_parallal_2.getNumPartitions() )

rdd_mp_2 = rdd_parallal_2.mapPartitions(total)
print(rdd_mp_2.collect())

# What is the default parallalism on this system?
print("Default number of partitions: ", sc.defaultParallelism)
sc.stop()
