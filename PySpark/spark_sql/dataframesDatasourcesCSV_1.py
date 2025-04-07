# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

#data_path = "C:\\PySpark\\data\\flight-data\\csv\\2010-summary.csv"
data_output_path = "C:\\PySpark\\data\\flight-data\\csv_out\\"
data_output_path_2 = "C:\\PySpark\\data\\flight-data\\csv_out_2\\"

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .getOrCreate()

csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("sep", "\t")\
  .option("inferSchema", "true")\
  .load(data_output_path)
  
csvFile.show(10)  
  
from pyspark.sql.functions import col

filterQry = col("DEST_COUNTRY_NAME") == "United States" 
csvFiltered = csvFile.where(filterQry)  
  
csvFiltered.write.format("csv")\
  .option("sep", "#")\
  .save(data_output_path_2)

#type(csvFile)  

spark.stop()