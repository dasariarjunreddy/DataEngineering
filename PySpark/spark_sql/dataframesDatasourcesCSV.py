# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

data_path = "C:\\PySpark\\data\\flight-data\\csv\\2010-summary.csv"
data_output_path = "C:\\PySpark\\data\\flight-data\\csv_out_saama1234591\\"

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .getOrCreate()

csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load(data_path)
  
from pyspark.sql.functions import col

filterQry = col("DEST_COUNTRY_NAME") == "United States" 
csvFiltered = csvFile.where(filterQry)  
  
csvFiltered.write.format("parquet")\
  .option("sep", "\t")\
  .save(data_output_path)

#type(csvFile)  

spark.stop()