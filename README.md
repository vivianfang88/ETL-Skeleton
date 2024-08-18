# **Data Ingestion Project**

## **Introduction**
Simple ETL skeleton is designed to ingest updated data from both the same and different Excel files.
The PySpark library serves as a bridge to read data from Excel files and ingest it into a PostgreSQL database.
Additionally, the ETL pipeline is automated using Apache Airflow as the scheduler.

## **Requirements**
- Ubuntu Server (WSL)
- Java 8
- Python3
- PostgreSQL <code style="color : aqua">or any other open-soure database</code>
- JDBC driver 42.7.3
- Libraries listed in requirements.txt


## **Usage**
1. Create a Virtual Environment:
Set up a virtual environment and install the necessary packages from requirements.txt.

2. Install Airflow:
Install Airflow using PyPI by following the official installation **[guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)**.
In this project, Airflow logs are stored in the same working directory for debugging purposes.

3. Ensure Data Source Availability:
Prepare your data source. In this project, dummy data was created in Excel with created_date and updated_date columns to capture updated data for later use.

4. Create ETL Script:
Write a Python script to read, transform, and ingest data into PostgreSQL.

5. Add Debugging Column:
Add a new column ingestion_timestamp to your table for debugging purposes.

6. Create DAG:
Set up an Airflow DAG to automate the entire ETL process.

