from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/opt/spark/resources/jars/postgresql-9.4.1207.jar"
model = "/opt/spark/resources/models/linear_model.sav"
postgres_db = "jdbc:postgresql://postgres/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="gbc-predict-equipment", 
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job_predict = SparkSubmitOperator(
    task_id="spark_job_predict",
    application="/opt/spark/app/gbc_predict.py", # Spark application path created in airflow and spark cluster
    name="predict-equipment-day",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[model, postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_predict >> end
