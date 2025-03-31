import os
import sys

def main():
    #pyspark-shell
    #os.system("pip install kafka-python")

    os.environ["SPARK_HOME"]="C:/spark/spark-3.1.2-bin-hadoop2.7"
    os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
    #os.environ["HADOOP_HOME"] ="/usr/lib/hadoop/bin"
    # In below two lines, use /usr/bin/python2.7 if you want to use Python 2
    #os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
    #os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
    sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
    sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

    #org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 pyspark-shell'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'
    #    Spark
    from pyspark import SparkContext
    #    Spark Streaming
    from pyspark.streaming import StreamingContext
    #    Kafka
    #from pyspark.streaming.kafka import KafkaUtils
    #    json parsing
    import json
    from pyspark.sql.types import StructType, IntegerType, StringType

    #sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    #sc.setLogLevel("WARN")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
      .appName("Spark Structured Streaming from Kafka") \
      .getOrCreate()


    emp_data = spark \
      .readStream.format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "twitter") \
      .load() \
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show()




if __name__=='__main__':
    main()

