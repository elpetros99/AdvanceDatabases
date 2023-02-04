# AdvanceDatabases

Requirements:
1. OS Linux Server
2. Follow th instructions in https://sparkbyexamples.com/hadoop/apache-hadoop-installation/ to install handoop 2.7
3. Follow the instructions in Spark_Install_instructions-1.pdf to install Apache Spark, Python 3.8, Pyspark, Java and to create workers on the cluster

Instructions to replicate:
1. Git Clone our project to your prefeered destination
2. Add datasets folder to the HDFS(Handoop) with command "hdfs dfs -put datasets"
3. Run the command "spark-submit query.py --deploy_mode cluster" where query is the filename of the query you want to run.
