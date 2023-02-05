from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, dayofyear, floor
import os
import sys
import datetime
import math
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
df_zone = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("datasets/extra/zone.csv")
df_taxis=spark.read.parquet("datasets/taxis/")

#Create RDD from external Data source
#rdd_taxis = spark.sparkContext.parquet("datasets/taxis/")
#rdd_zone = spark.sparkContext.csv("datasets/extra/zone.csv")

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

#Zone Schema
# |-- LocationID: integer (nullable = true)
# |-- Borough: string (nullable = true)
# |-- Zone: string (nullable = true)
# |-- service_zone: string (nullable = true)


#df_taxis2 = df_taxis.withColumn("day", dayofyear("tpep_pickup_datetime")).select("day", "tpep_pickup_datetime", "trip_distance", "total_amount", "PULocationID", "DOLocationID")
#PUjoin = df_taxis2.join(df_zone,df_taxis2.PULocationID==df_zone.LocationID)
#df_zone2 = df_zone.withColumnRenamed("LocationID", "LocationID2").withColumnRenamed("Borough", "Borough2").withColumnRenamed("Zone", "Zone2").withColumnRenamed("service_zone", "service_zone2")

#big_df = PUjoin.join(df_zone2, PUjoin.DOLocationID==df_zone2.LocationID2)



############## Q1 ################################################################
#q1 = big_df.select("PULocationID", "Zone", "DOLocationID", "Zone2", "tip_amount").filter((col("month") == 3) & (col("Zone2") == "Battery Park")).orderBy(desc("tip_amount")).show(1)

################# Q2 ##############################################################

#max_tolls_per_month = big_df.filter(col("tolls_amount") != "0.0").groupBy("month").agg(max("tolls_amount").alias("max_tolls_amount")).withColumnRenamed("month", "month2")

#combined = max_tolls_per_month.join(big_df, (big_df.month==max_tolls_per_month.month2) & (big_df.tolls_amount==max_tolls_per_month.max_tolls_amount))

#combined.select("PULocationID","Zone", "DOLocationID", "Zone2", "month", "max_tolls_amount").orderBy(asc("month")).show()

################# a different (not ideal) version of Q3 rdd #####################################

#rdd = df_taxis2.rdd

#rdd = rdd.filter(lambda x: x.PULocationID is not x.DOLocationID).map(lambda x: (x.day, x.trip_distance, x.total_amount))

#rdd = rdd.map(lambda x: (int(x[0]/15), (x[1], x[2], 1)))


#rdd = rdd.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2])).map(lambda x: (x[0], x[1][0]/x[1][2], x[1][1]/x[1][2]))


#for y in rdd.collect():
#   print(y)

############### the correct version of Q3 rdd with df_taxis #################################
rdd = df_taxis.rdd


start_time = time()

rdd = rdd.filter(lambda x: x.PULocationID is not x.DOLocationID).map(lambda x: (x.tpep_pickup_datetime, x.trip_distance, x.total_amount)).filter(lambda x: x[0].year == 2022)


rdd = rdd.map(lambda x: (x[0].toordinal() - datetime.datetime(2022, 1, 1).toordinal(), x[1], x[2]))


rdd = rdd.map(lambda x: (int(x[0]/15), (x[1], x[2], 1)))


#rdd = rdd.reduceByKey(lambda acc, x: (acc[0] + x[0], acc[1] + x[1], acc[2] + x[2]))


#rdd = rdd.map(lambda x: (x[0], x[1][0]/x[1][2], x[1][1]/x[1][2], x[1][2])).sortBy(lambda x: x[0])


#group by key has high memory complexity so it dows not fit our purposes-memory exceded
rdd = rdd.reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
rdd = rdd.map(lambda x: (x[0], x[1][0]/x[1][2], x[1][1]/x[1][2])).sortBy(lambda x: x[0])
res = rdd.collect()

end_time = time()

elapsed_time = end_time - start_time
print("Elapsed time: " +str(elapsed_time) +" seconds")

for y in res:
   print(y)
elapsed_time = end_time - start_time
print("====================================================================")
print("Elapsed time: " +str(elapsed_time) +" seconds")
print("====================================================================")
