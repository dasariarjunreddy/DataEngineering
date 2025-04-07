# -*- coding: utf-8 -*-
import findspark
findspark.init()

#from pyspark.sql import SparkSession
#import com.mysql.jdbc.Driver
#from pyspark.sql import SaveMode

#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.jars", "C:\\Pyspark\\spark_sql\\mysql-connector-java-5.1.49.jar").master("local").appName("PySpark_MySQL_test").getOrCreate()

jdbcDF = spark.read.format("jdbc")\
                    .option("url","jdbc:mysql://localhost:3306/sparkdb")\
                    .option("driver", "com.mysql.jdbc.Driver")\
                    .option("dbtable", "emp")\
                    .option("user", "root")\
                    .option("password", "root")\
                    .load()
  
jdbcDF.createOrReplaceTempView("empTempView")
spark.sql("SELECT * FROM empTempView").show()

# Snippet 2:  Writing to MySQL using JDBC
spark.sql("SELECT * FROM empTempView")\
        .write\
        .format("jdbc")\
        .option("url", "jdbc:mysql://localhost:3306/sparkdb")\
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "emp999")\
        .option("user", "root")\
        .option("password", "root")\
        .save()

spark.stop()