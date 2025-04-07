# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

data_path = "C:\\PySpark\\data\\"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Basics") \
    .config("spark.executor.memory", "2g") \
    .config("spark.master", "local") \
    .getOrCreate()
 
"""
sc = spark.sparkContext
rdd1 = sc.parallelize(range(100)) # converting an existing collection
text_file = sc.textFile(os.path.join(data_path, "wordcount.txt") ) # from a textfile

spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
"""
sc = spark.sparkContext
rdd1 = sc.textFile("")

df = spark.read.json(data_path + "users.json")

df.show()

df.schema

# print the schema of the dataframe
df.printSchema()

# dataframe operations
df.select("name").show()

df.select(df['name'], df['age'] + 1).show()

df.where(df['age'] > 20).show()
df.where((df['age'] > 20) | (df['age'].isNull())).show()

df.groupBy("age").count().show()
df.filter(df['age'].isNotNull()).groupBy("age").count().show()

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("users123")

sqlDF = spark.sql("SELECT * FROM users123 where age >= 20")
sqlDF.show()

# Register the DataFrame as a global temporary view
df.createGlobalTempView("gtv_users")

# Global temporary view is tied to a system preserved database `global_temp`
gtvDF = spark.sql("SELECT * FROM global_temp.gtv_users")
gtvDF.show()

gtvDF2 = spark.newSession().sql("SELECT * FROM global_temp.gtv_users")
gtvDF2.show()


spark.stop()