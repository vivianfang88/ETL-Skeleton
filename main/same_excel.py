import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, col
import pandas as pd
from vars.config import (postgres_drive_class_path, ingestion_excel_path, postgres_username, postgres_password, postgres_url)



# Initialise Spark
spark = SparkSession.builder \
    .appName("Excel to PostgreSQL Pipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .config("spark.driver.extraClassPath",postgres_drive_class_path) \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
    .getOrCreate()


# Read Data    
df = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Sheet1'!A1") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(ingestion_excel_path) 


# Add the _lanfing_loaded_at column with the current timestamp
landing_loading_timestamp =  df.withColumn("_landing_loaded_at", current_timestamp()) 

#filter latest and updated data
filter_latest_data = landing_loading_timestamp.filter(col('updated_at').cast('date') == current_date()) #TODO: IF CONDITION FOR FIRST TIME INGEST AND LATEST DATA INGEST (CI/CD pipeline)

#final data
filter_latest_data.printSchema()
filter_latest_data.show()
final_data = filter_latest_data

# JDBC URL and Properties
properties = {
    "user": postgres_username,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}



# Write to PostgreSQL 
#TODO: WRITE A SEPARATE DYNAMIC FUNCTION FOR FULL REFRESH / INCREMENTAL APPROACH
final_data.write \
    .format("jdbc") \
    .option("driver",properties["driver"]) \
    .option("url", postgres_url) \
    .option("dbtable", "poc.landing_same_excel_table") \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .mode("append") \
    .save()

# Stop the SparkSession
spark.stop()
