from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
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
df_zone = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("datasets/extra/zone.csv")
df_taxis=spark.read.parquet("datasets/taxis/")

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


########################### Q1 ##############################
#q1=df_taxis

#q1=q1.join(df_zone,q1.DOLocationID==df_zone.LocationID).withColumn("month", month("tpep_pickup_datetime"))
##q1=q1.join(df_zone,q1.DOLocationID==df_zone1.LocationID1)

#q1.select("PULocationID","DOLocationID","tip_amount").filter((col("month") == 3) & (col("Zone") == "Battery Park")).orderBy(desc("tip_amount")).show(1)

#q1.collect()



df_taxis2 = df_taxis.withColumn("month", month("tpep_pickup_datetime"))
PUjoin = df_taxis2.join(df_zone,df_taxis2.PULocationID==df_zone.LocationID)
df_zone2 = df_zone.withColumnRenamed("LocationID", "LocationID2").withColumnRenamed("Borough", "Borough2").withColumnRenamed("Zone", "Zone2").withColumnRenamed("service_zone", "service_zone2")

big_df = PUjoin.join(df_zone2, PUjoin.DOLocationID==df_zone2.LocationID2)



############## Q1 ################################################################
q1 = big_df.select("PULocationID", "Zone", "DOLocationID", "Zone2", "tip_amount").filter((col("month") == 3) & (col("Zone2") == "Battery Park")).orderBy(desc("tip_amount")).show(1)





#################################################################################
#q2 = q2.withColumn("month", month("tpep_pickup_datetime")).filter(col("tolls_amount") != "0.0").groupBy("month").agg(max("tolls_amount").alias("max_tolls_amount")).select("PULocationaID", "DOLocationID").orderBy(asc("month"))

#q2.show()

################# Q2 ##############################################################
#it is working normally, only that it shows the location ids instead of the location names
#df_taxis2 = df_taxis.withColumn("month", month("tpep_pickup_datetime"))

#i renamed month to month2 because for some reason when i join the 2 tables, to column month gets written twice so it gets ambiguous
#max_tolls_per_month = big_df.filter(col("tolls_amount") != "0.0").groupBy("month").agg(max("tolls_amount").alias("max_tolls_amount")).withColumnRenamed("month", "month2")

#combined = max_tolls_per_month.join(big_df, (big_df.month==max_tolls_per_month.month2) & (big_df.tolls_amount==max_tolls_per_month.max_tolls_amount))

#combined.select("PULocationID","Zone", "DOLocationID", "Zone2", "month", "max_tolls_amount").orderBy(asc("month")).show()

##################################################################################

###########test in sql###############
#q2 = df_taxis.withColumn("month", month("tpep_pickup_datetime"))
#q2.createOrReplaceTempView("data")

#q2sql = spark.sql("select max(tolls_amount),month from data where (tolls_amount != 0.0) group by month order by month").show()

#q2sql = spark.sql("select PULocationID, max(tolls_amount) as maxt, month from data where (tolls_amount != 0.0) group by PULocationID, month")

#q2sql.createOrReplaceTempView("data2")

#sql2 = spark.sql("select maxt, month from data2 group by month").show()

#### chat gpt test###
#qn2 = df_taxis.withColumn("month", month("tpep_pickup_datetime"))
#qn2.createOrReplaceTempView("data")

#q2sql = spark.sql("WITH max_tolls_amounts AS (SELECT month, MAX(tolls_amount) AS max_tolls_amount FROM data GROUP BY month) SELECT data.month, data.PULocationID, data.DOLocationID, data.tolls_amount FROM data JOIN max_tolls_amounts ON ( data.month = max_tolls_amounts.month AND data.tolls_amount = max_tolls_amounts.max_tolls_amount)").show()
