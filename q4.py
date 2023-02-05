from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys, time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from time import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#details for setting up a spark standalone cluster and using the DataFrames API
# https://spark.apache.org/docs/latest/spark-standalone.html
# https://spark.apache.org/docs/2.2.0/sql-programming-guide.html

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("q4").getOrCreate()
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


###########Q3##################

#remove rows with same arrival and destination

#q3=df_taxis
#cols = ['PULocationID', 'DOLocationID']
#q3=q3.withColumn('arr', array_sort(array(*cols))).drop_duplicates(['arr']).drop('arr')
#q3 = q3.groupBy(window("tpep_pickup_datetime", "15 days")).agg(avg("trip_distance").alias("distance"),avg("fare_amount").alias("cost"))
#q3.select(q3.window.start.cast("string").alias("start"),q3.window.end.cast("string").alias("end"),"distance","cost").show()
###########Q3##################

start_time = time()
############################Q4##########################
q4=df_taxis.withColumn("Hours",hour("tpep_pickup_datetime")).withColumn("Days",date_format("tpep_pickup_datetime","E")).groupBy("Days","Hours").agg(max("passenger_count").alias("passengers"))
w = Window.partitionBy("Days").orderBy(desc("passengers"))
q4 = q4.withColumn("rn", row_number().over(w)).filter("rn <= 3")
q4.select("Hours","Days","passengers").collect()

fin_time = time()

q4.select("Hours","Days","passengers","rn").show(22)


msg="Time elapsed for q4 is %.4f sec.\n" % (fin_time-start_time) 
print(msg)
with open("times-sql.txt", "a") as outfile:
	outfile.write(msg)


#.groupBy("Hours").agg(max("passenger_count").alias("passengers")).orderBy(desc("passengers")).select("Hours","passengers").show()
###########################Q5###########################
#Q5
print("======================================================================")

#################Q5############
#df=df_taxis.withColumn("sub",(df_taxis["fare_amount"]/df_taxis["tip_amount"])).withColumn("Month",month("tpep_pickup_datetime")).withColumn("Day",dayofmonth("tpep_pickup_datetime"))
#df=df.na.drop(subset=["sub"])
#w = Window.partitionBy("Month").orderBy(desc("sub"))
#df = df.withColumn("rn", row_number().over(w)).filter("rn <= 3")
#df.select("Month","Day","rn","sub").show()
#######################Q5###########

#.agg(avg("sub").alias("final")).orderBy(desc("final")).select("Day","final").show(5)
print("======================================================================")
#print(df_taxis)
