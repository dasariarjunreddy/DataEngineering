# -*- coding: utf-8 -*-
import findspark
findspark.init()

from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row

warehouse_location = abspath('C:\\PySpark\\spark-warehouse')

import os
import sys

os.environ["SPARK_HOME"]="C:\\spark-2.4.3-bin-hadoop2.6"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "\\python\\lib"
os.environ["HADOOP_HOME"] ="C:\\spark-2.4.3-bin-hadoop2.6"
os.environ["PATH"] =os.environ["HADOOP_HOME"]+"\\bin"
os.environ["JAVA_HOME"]="C:\\Program Files\\Java\jdk1.8.0_25"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "C:\\python-3.7.7-embed-amd64" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\python-3.7.7-embed-amd64"
sys.path.insert(0, os.environ["PYLIB"] +"\\py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"\\pyspark.zip")

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-hive_2.11:2.3.3 pyspark-shell'

spark = SparkSession \
    .builder \
    .appName("Datasources") \
    .config("spark.master", "local") \
    .config("spark.sql.warehouse.dir", warehouse_location)\
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("show databases").show()

spark.sql("CREATE TABLE IF NOT EXISTS src5 (key INT, value STRING) USING hive")

spark.sql("show tables").show()

spark.sql("load data local inpath 'examples\src\main\resources\kv1.txt' overwrite INTO TABLE src5")

spark.sql("SELECT * FROM src3").show()

spark.sql("SELECT COUNT(*) FROM src").show()

sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

stringsDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))

for record in stringsDS.collect():
    print(record)
    
Record = Row("key", "value")
recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
recordsDF.createOrReplaceTempView("records")

spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

spark.sql("drop table src3")
spark.stop()