import os
from datetime import datetime, timedelta
import subprocess
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from vars.common import ingest_same_excel_path



default_start_date = pendulum.datetime(2024,1,1, tz="Asia/Singapore")


#Define default arguments
default_args = {
 'owner': 'vivian',
 'start_date':default_start_date,
 'retries': 2,
 'retry_delay':timedelta(seconds=15),
 'depends_on_past' :False
}

# Instantiate your DAG
dag = DAG (
    'ingest_to_postgres_same_file',
    description="test ingest updated data from the same excel to postgres",
    default_args=default_args, 
    schedule_interval = None
    # schedule_interval="0 10 * * *" # run daily at 10am Malaysia time
    )

def run_main_script():
    subprocess.run(["python",ingest_same_excel_path], check=True)

run_ingestion_code = PythonOperator(
 task_id='run_ingest_same_file',
 python_callable=run_main_script,
 dag=dag,
)


# Set task dependencies
run_ingestion_code 