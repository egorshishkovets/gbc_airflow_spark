from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/opt/spark/resources/jars/postgresql-9.4.1207.jar"
equipment_file = "/opt/spark/resources/data/gbc_repair_add.csv"
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
        dag_id="gbc-load-day", 
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/opt/spark/app/gbc_load_day.py", # Spark application path created in airflow and spark cluster
    name="load-equipment-day",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[equipment_file,postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

trigger_scoring = TriggerDagRunOperator(
    task_id='trigger_scoring',
    trigger_dag_id='gbc-predict-equipment',
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_postgres >> trigger_scoring  >> end
