from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour,dayofyear
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from time import time
import datetime
from time import mktime as mktime
from pyspark.sql.functions import floor

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
count = df_taxis.count()
#df_taxis.show(5)
# Print the count
print(count)
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
start_time = time()

#generate starttime for window
start_date = datetime.datetime(2022, 1, 2, 20, 0, 0) #because of our timezone
days_since_1970_to_start_date =int(mktime(start_date.timetuple())/86400)
offset_days = days_since_1970_to_start_date % 7
#################

q3=df_taxis
q3=q3.filter(col("tpep_pickup_datetime") >= "2022-01-01")
q3=q3.filter(col("PULocationID") != col("DOLocationID"))
#q3.select(q3.tpep_pickup_datetime).sort(q3.tpep_pickup_datetime.asc()).show()
#cols = ['PULocationID', 'DOLocationID']
#q3=q3.withColumn('arr', array_sort(array(*cols))).drop_duplicates(['arr']).drop('arr')
#q3=q3.withColumn("day_year",dayofyear(q3["tpep_pickup_datetime"])).withColumn
q3 = q3.withColumn("day_of_year", floor(dayofyear(q3["tpep_pickup_datetime"])/15))
q3 = q3.groupBy(q3["day_of_year"]).agg(min(q3["tpep_pickup_datetime"]).alias("starting"), max(q3["tpep_pickup_datetime"]).alias("ending"),avg("trip_distance").alias("distance"),avg("total_amount").alias("cost"))
#q3 = q3.groupBy(window("tpep_pickup_datetime", "15 days",startTime='{} days'.format(offset_days))).agg(avg("trip_distance").alias("distance"),avg("total_amount").alias("cost"))
#q3.select(q3.window.start.cast("string").alias("start"),q3.window.end.cast("string").alias("end"),"distance","cost").collect()
q3.select("starting","ending","distance","cost").collect()
end_time = time()
elapsed_time = end_time - start_time



#q3=q3.select(q3.window.start.cast("string").alias("start"),q3.window.end.cast("string").alias("end"),"distance","cost")
q3=q3.select("starting","ending","distance","cost")
q3.sort(q3.starting.asc()).show(30)
print("Elapsed time: " +str(elapsed_time) +" seconds")
###########Q3##################


############################Q4##########################
#q4=df_taxis.withColumn("Hours",hour("tpep_pickup_datetime")).withColumn("Days",date_format("tpep_pickup_datetime","E")).groupBy("Days","Hours").agg(max("passenger_count").alias("passengers"))
#w = Window.partitionBy("Days").orderBy(desc("passengers"))
#q4 = q4.withColumn("rn", row_number().over(w)).filter("rn <= 3")
#q4.select("Hours","Days","passengers").show()




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
