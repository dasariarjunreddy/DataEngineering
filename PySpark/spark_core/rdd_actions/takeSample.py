# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "takeSample")

rdd = sc.parallelize(range(0, 30))

# takeSample(withReplacement, num, seed=None)

sample1 = rdd.takeSample(True,20)
print("Sample 1",sample1)

sample2 = rdd.takeSample(False, 5)
print("sample2",sample2)

sample3 = rdd.takeSample(False, 15, 5)
print("Sample 3: ",sample3)

sc.stop()

