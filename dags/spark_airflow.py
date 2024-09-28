import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id = "sparking_flow",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval=None
)

create_temp_pg_table = PostgresOperator(
    task_id='create_temp_pg_table',
    sql='''
        DROP TABLE IF EXISTS sales_temp;
        CREATE TABLE sales_temp (
            sale_id INT PRIMARY KEY,
            customer_id INT,
            product_id INT,
            quantity INT,
            sale_date DATE,
            sale_amount INT,
            region VARCHAR(5)
        );
    ''',
    params={'PGPASSWORD': 'password'},
    postgres_conn_id='postgres_conn_id',
    dag=dag
)

def call_file_gd(**kwargs):
    import subprocess
    subprocess.run(['python', 'jobs/python/generate_data.py'])

task_generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=call_file_gd,
    op_kwargs= {'num_sales': 10, 'num_products': 10},
    dag=dag,
)

task_clear_data = SparkSubmitOperator(
    task_id="clear_data",
    conn_id="spark-conn",
    application="jobs/python/clear_data.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

create_temp_pg_table >> task_generate_data >> task_clear_data >> end