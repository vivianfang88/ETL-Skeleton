import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date
import pandas as pd
from datetime import datetime as dt
import re
from vars.config import (postgres_drive_class_path, diff_excel_input_directory, postgres_username, postgres_password, postgres_url)

today_file_path = []

# Initialise Spark
spark = SparkSession.builder \
    .appName("Excel to PostgreSQL Pipeline") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .config("spark.driver.extraClassPath",postgres_drive_class_path) \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.1_0.20.4")\
    .getOrCreate()


# filter today date excel
today_date = dt.today().strftime('%Y%m%d')
for filename in os.listdir(diff_excel_input_directory):
    if filename.endswith('.xlsx'):
        match = re.search(r'diff_file_ingestion_(\d{8})\.xlsx$', filename)
        if match:
            file_date = match.group(1)

            if  file_date == today_date:
                file_path = os.path.join(diff_excel_input_directory, filename)
                today_file_path.append(file_path)


if len(today_file_path) == 1:
    # Read Data    
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("dataAddress", "'Sheet1'!A1") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(today_file_path) 


    # Add the _lanfing_loaded_at column with the current timestamp
    landing_loading_timestamp =  df.withColumn("_landing_loaded_at", current_timestamp()) 


    #final data
    landing_loading_timestamp.printSchema()
    landing_loading_timestamp.show()
    final_data = landing_loading_timestamp

    # JDBC URL and Properties
    properties = {
        "user": postgres_username,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
    }



    # Write to PostgreSQL 
    final_data.write \
        .format("jdbc") \
        .option("driver",properties["driver"]) \
        .option("url", postgres_url) \
        .option("dbtable", "poc.landing_diff_excel") \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .mode("append") \
        .save()

    # Stop the SparkSession
    spark.stop()

else:
    raise Exception("no new excel file found / there are multiple excel files with the same date")



