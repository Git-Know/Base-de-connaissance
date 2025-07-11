from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToDB") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate() 

spark.sparkContext.setLogLevel("WARN")
