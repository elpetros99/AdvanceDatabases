from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
#df_taxis.printSchema()
#df_taxis.show(10) 
#DataFrame transformations and actions

print("extra")
#df.printSchema()
#df.show()

#dF_TAXIS SCHEMA
# |-- VendorID: long (nullable = true)
# |-- tpep_pickup_datetime: timestamp (nullable = true)
# |-- tpep_dropoff_datetime: timestamp (nullable = true)
# |-- passenger_count: double (nullable = true)
# |-- trip_distance: double (nullable = true)
# |-- RatecodeID: double (nullable = true)
# |-- store_and_fwd_flag: string (nullable = true)
# |-- PULocationID: long (nullable = true)
# |-- DOLocationID: long (nullable = true)
# |-- payment_type: long (nullable = true)
# |-- fare_amount: double (nullable = true)
# |-- extra: double (nullable = true)
# |-- mta_tax: double (nullable = true)
# |-- tip_amount: double (nullable = true)
# |-- tolls_amount: double (nullable = true)
# |-- improvement_surcharge: double (nullable = true)
# |-- total_amount: double (nullable = true)
# |-- congestion_surcharge: double (nullable = true)
# |-- airport_fee: double (nullable = true)


#Q4


df_taxis.withColumn("Hours",hour("tpep_pickup_datetime")).groupBy("Hours").agg(max("passenger_count").alias("passengers")).orderBy(desc("passengers")).select("Hours","passengers").show()
#Q5
print("======================================================================")

#df_taxis.withColumn("sub",(df_taxis["fare_amount"]-df_taxis["tip_amount"])/df_taxis["fare_amount"]).withColumn("Day",dayofmonth("tpep_pickup_datetime")).groupBy("Day").agg(avg("sub").alias("final")).orderBy(desc("final")).select("Day","final").show(5)
print("======================================================================")
