# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext
import os

sc = SparkContext("local", "map")

data_path = "C:\\PySpark\\data"

dirPath = os.path.join(data_path, "files411119")
os.mkdir(dirPath)

with open(os.path.join(dirPath, "1.txt"), "w") as file1:
    _ = file1.write("1.. some text")
    
with open(os.path.join(dirPath, "2.txt"), "w") as file2:
    _ = file2.write("2.. some other text")
    
textFiles = sc.wholeTextFiles(dirPath)

output1 = sorted(textFiles.collect())
print(output1)

sc.stop()