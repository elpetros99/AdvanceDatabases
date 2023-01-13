from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from time import time


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#details for setting up a spark standalone cluster and using the DataFrames API
# https://spark.apache.org/docs/latest/spark-standalone.html
# https://spark.apache.org/docs/2.2.0/sql-programming-guide.html

spark = SparkSession.builder.master("spark://192.168.0.2:7077").getOrCreate()
print("spark session created")

#read a sample input file in CSV format from local disk
df_zone = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("hdfs://master:9000/datasets/extra/zone.csv")
df_taxis=spark.read.parquet("hdfs://master:9000/datasets/taxis/")

#print("Taxis")
#df_taxis.printSchema()
#df_taxis.show(10) 
#DataFrame transformations and actions

# Taxis Schema
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

#print("extra")
#df.printSchema()
#df.show(100)
#Zone Schema
# |-- LocationID: integer (nullable = true)
# |-- Borough: string (nullable = true)
# |-- Zone: string (nullable = true)
# |-- service_zone: string (nullable = true)


start_time = time()
df_taxis2 = df_taxis.withColumn("month", month("tpep_pickup_datetime"))
PUjoin = df_taxis2.join(df_zone,df_taxis2.PULocationID==df_zone.LocationID)
df_zone2 = df_zone.withColumnRenamed("LocationID", "LocationID2").withColumnRenamed("Borough", "Borough2").withColumnRenamed("Zone", "Zone2").withColumnRenamed("service_zone", "service_zone2")

big_df = PUjoin.join(df_zone2, PUjoin.DOLocationID==df_zone2.LocationID2)

################# Q2 ##############################################################
#it is working normally, only that it shows the location ids instead of the location names

#i renamed month to month2 because for some reason when i join the 2 tables, to column month gets written twice so it gets ambiguous

max_tolls_per_month = big_df.filter(col("tolls_amount") != "0.0").groupBy("month").agg(max("tolls_amount").alias("max_tolls_amount")).withColumnRenamed("month", "month2")

combined = max_tolls_per_month.join(big_df, (big_df.month==max_tolls_per_month.month2) & (big_df.tolls_amount==max_tolls_per_month.max_tolls_amount))

combined.select("PULocationID","Zone", "DOLocationID", "Zone2", "month", "max_tolls_amount").orderBy(asc("month")).collect()
end_time = time()

print("Elapsed time: "+ str(elapsed_time)+ " seconds")

combined.select("PULocationID","Zone", "DOLocationID", "Zone2", "month", "max_tolls_amount").orderBy(asc("month")).show()
print("Elapsed time: "+ str(elapsed_time)+ " seconds")

##################################################################################


