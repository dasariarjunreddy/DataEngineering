# -*- coding: utf-8 -*-
import findspark
findspark.init()

import os
import shutil 
import sys
from pyspark import SparkContext

data_path = "C:\\PySpark\\data"
#output_path = "C:\\PySpark\\data"
output_dir = "output"
output_path = os.path.join(data_path, output_dir)

sc = SparkContext("local", "map")

#########################################
#   map - Example 1
#########################################
rdd_data = sc.parallelize([1,2,3,5,6,7,8,9,10],3) #123 567 8910

rdd_map_1 = rdd_data.map(lambda x: [x,x,x])
for x in rdd_map_1.collect(): print(x) 

rdd_map_f = rdd_data.flatMap(lambda x: [x,x,x])
for x in rdd_map_f.collect(): print(x) 

rdd_map_2 = rdd_data.map(lambda x: x**3)
for x in rdd_map_2.collect(): print(x) 

#########################################
#   map - Example 2
#########################################
baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")
baby_names = sc.textFile(baby_names_csv_file,3)
rows = baby_names.map(lambda line: line.split(","))
print("Total number of Rows:", rows.count())
for row in rows.take(10): print(row[1].lower())

# get names and their length
rows2 = rows.map(lambda line: (line[1], len(line[1])))
if (os.path.isdir(output_path)):
    shutil.rmtree(output_path)                                                      
#rows2.saveAsTextFile("C:\\Pyspark\\data\\banamesoutput99123\\")
rows2.saveAsTextFile(output_path)                                                      
for x in rows2.take(10): print(x)

sc.stop()