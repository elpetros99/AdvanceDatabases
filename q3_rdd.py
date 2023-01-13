from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, dayofyear, floor
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window


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


df_taxis2 = df_taxis.withColumn("day", dayofyear("tpep_pickup_datetime")).select("day", "tpep_pickup_datetime", "trip_distance", "total_amount", "PULocationID", "DOLocationID")
#PUjoin = df_taxis2.join(df_zone,df_taxis2.PULocationID==df_zone.LocationID)
#df_zone2 = df_zone.withColumnRenamed("LocationID", "LocationID2").withColumnRenamed("Borough", "Borough2").withColumnRenamed("Zone", "Zone2").withColumnRenamed("service_zone", "service_zone2")

#big_df = PUjoin.join(df_zone2, PUjoin.DOLocationID==df_zone2.LocationID2)



############## Q1 ################################################################
#q1 = big_df.select("PULocationID", "Zone", "DOLocationID", "Zone2", "tip_amount").filter((col("month") == 3) & (col("Zone2") == "Battery Park")).orderBy(desc("tip_amount")).show(1)

################# Q2 ##############################################################

#max_tolls_per_month = big_df.filter(col("tolls_amount") != "0.0").groupBy("month").agg(max("tolls_amount").alias("max_tolls_amount")).withColumnRenamed("month", "month2")

#combined = max_tolls_per_month.join(big_df, (big_df.month==max_tolls_per_month.month2) & (big_df.tolls_amount==max_tolls_per_month.max_tolls_amount))

#combined.select("PULocationID","Zone", "DOLocationID", "Zone2", "month", "max_tolls_amount").orderBy(asc("month")).show()

################# Q3 rdd ######################################################
#  Row(day=1, tpep_pickup_datetime=datetime.datetime(2022, 1, 1, 23, 45, 25))
#  (datetime.datetime(2022, 1, 1, 23, 26, 21), 3.0, 22.38)
#  (9, 1.9, 12.98)

rdd = df_taxis2.rdd

rdd = rdd.filter(lambda x: x.day == 40)


rdd = rdd.filter(lambda x: x.PULocationID is not x.DOLocationID).map(lambda x: (x.day, x.trip_distance, x.total_amount))
#.map(lambda x: (floor(x[0]/15), x[1], x[2]))

rdd = rdd.groupByKey().mapValues(lambda x: (sum(x[0])/len(x[0]), sum(x[1])/len(x[1])))
#i = 0
for y in rdd.collect():
   print(y)

