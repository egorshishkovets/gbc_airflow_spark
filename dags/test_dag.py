from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_python(name: str):
    print(f"Hello {name}!")

###############################################
# DAG Definition
###############################################
now = datetime.now()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    #"start_date": datetime(2019,4,1),
    #"end_date": datetime(2019,12,1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


with DAG(
        dag_id="test_dag",
        #schedule_interval="@once",
        #catchup=False,
        default_args=default_args, 
        schedule_interval=timedelta(1)
) as dag:

    start = DummyOperator(task_id="start")

    python_example = PythonOperator(
        task_id=f"python_example",
        python_callable=test_python,
        op_kwargs={
            "name": f"GBC",
        }
    )
    
    end = DummyOperator(task_id="end")

    start >> python_example  >> end

    #python_example1 = PythonOperator(
    #    task_id=f"python_example1",
    #    python_callable=test_python,
    #    op_kwargs={
    #        "name": f"GBC_parallel",
    #    }
    #)
    #for i in range(4):
    #    python_example_1 = PythonOperator(
    #        task_id=f"python_example_{i}",
    #        python_callable=test_python,
    #        op_kwargs={
    #            "name": f"GBC_{i}",
    #        }
    #    )



    #    start >> python_example >> python_example_1  >> end
