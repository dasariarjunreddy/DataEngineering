# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext
 
data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "repartition")

rdd1 = sc.parallelize(range(1,100), 2)
print("rdd1 partitions: {}".format( rdd1.getNumPartitions() ) )

rdd2 = rdd1.repartition(5)
print("rdd2 partitions: {}".format( rdd2.getNumPartitions() ) )

rdd3 = rdd2.coalesce(3)
print("rdd3 partitions: {}".format( rdd3.getNumPartitions() ) )

rdd4 = rdd3.coalesce(2,True)    # ???? 
print("rdd4 partitions: {}".format( rdd4.getNumPartitions() ) ) # ???

rdd5 = rdd4.repartition(5)
print("rdd5 partitions: {}".format( rdd5.getNumPartitions() ) )

sc.stop()
