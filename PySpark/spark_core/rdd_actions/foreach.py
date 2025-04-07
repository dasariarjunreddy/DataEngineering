# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "aggregate")

numbers = [8,40,20,30,60,90]

numbersRDD = sc.parallelize(numbers)
def f(x): print(x)
ferdd=numbersRDD.foreach(f)

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
def f(x): print(x)
fore = words.foreach(f) 
for w in words.toLocalIterator():
    print(w)
sc.stop()