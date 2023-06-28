import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


default_args = {
  "owner": "airflow",    
  #"start_date": airflow.utils.dates.days_ago(2),
  # "end_date": datetime(),
  # "depends_on_past": False,
  # "email": ["airflow@example.com"],
  # "email_on_failure": False,
  #"email_on_retry": False,
  # If a task fails, retry it once after waiting
  # at least 5 minutes
  #"retries": 1,
  "retry_delay": timedelta(minutes=5),
}

dag_spark = DAG(
  dag_id = "thrive_assessment",
  default_args = default_args,
  # schedule_interval ="0 0 * * *",
  schedule ="@once",
  dagrun_timeout = timedelta(minutes=60),
  description ="thrive assessment description",
  start_date = airflow.utils.dates.days_ago(1)
)

spark_submit_local = SparkSubmitOperator(
  application ="/usr/local/src/python/ThriveAssessment.py",
  conn_id = "spark_local",
  task_id = "spark_submit_task",
  packages = "com.crealytics:spark-excel_2.11:0.13.7",
  dag = dag_spark
)

if __name__ == "__main__":
  dag_spark.cli()