from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#details for setting up a spark standalone cluster and using the DataFrames API
# https://spark.apache.org/docs/latest/spark-standalone.html
# https://spark.apache.org/docs/2.2.0/sql-programming-guide.html

spark = SparkSession.builder.master("spark://192.168.0.2:7077").getOrCreate()
print("spark session created")

#read a sample input file in CSV format from local disk
df = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("datasets/extra/zone.csv")
df_taxis=spark.read.parquet("datasets/taxis/")

print("Taxis")
df_taxis.printSchema()
df_taxis.show(10) 
#DataFrame transformations and actions

print("extra")
df.printSchema()
df.show(10)
