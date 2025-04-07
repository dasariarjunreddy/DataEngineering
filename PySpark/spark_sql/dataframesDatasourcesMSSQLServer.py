# -*- coding: utf-8 -*-
import findspark
findspark.init()

#from pyspark.sql import SparkSession
#import com.mysql.jdbc.Driver
#from pyspark.sql import SaveMode

#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.jars", "C:\\Pyspark\\spark_sql\\mssql-jdbc-6.4.0.jre8.jar").master("local").appName("PySpark_SQLServer_test").getOrCreate()

jdbcDF = spark.read.format("jdbc")\
                    .option("url","jdbc:sqlserver://localhost:1433/tekcruxdb")\
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                    .option("dbtable", "employee")\
                    .option("user", "user_name")\
                    .option("password", "")\
                    .load()
  
jdbcDF.createOrReplaceTempView("empTempView")
spark.sql("SELECT * FROM empTempView").show()

# Snippet 2:  Writing to MySQL using JDBC
spark.sql("SELECT * FROM empTempView")\
        .write\
        .format("jdbc")\
        .option("url", "jdbc:sqlserver://localhost:1433/tekcruxdb")\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("dbtable", "emp_result")\
        .option("user", "user_name")\
        .option("password", "")\
        .save()

spark.stop()