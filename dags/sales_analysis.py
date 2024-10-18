import airflow
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from operators.generate_data import generate_data
from operators.clickhouse_transfer import clickhouse_transfer

@dag(
    dag_id="sales_analysis",
    schedule_interval="45 9 * * 2",
    start_date=airflow.utils.dates.days_ago(1),
    tags=["sales"],
)

def sales_analysis():

    create_pg_tables = SQLExecuteQueryOperator(
        task_id="pg_sales_tables",
        sql="sql/pg_sales_tables.sql",
        conn_id="postgres_conn_id",
    )

    @task
    def generate():
        return generate_data(num_sales=1000000, num_products=10)

    task_clear_data = SparkSubmitOperator(
        task_id="clear",
        conn_id="spark-conn",
        application="plugins/operators/clear_data.py",
    )

    aggregation = SQLExecuteQueryOperator(
        task_id="aggregate",
        sql="sql/pg_aggr_table.sql",
        conn_id="postgres_conn_id",
    )

    @task
    def ch_transfer():
        clickhouse_transfer()

    data_generation = generate()
    
    create_pg_tables >> data_generation >> task_clear_data >> aggregation >> ch_transfer()

dag_instance = sales_analysis()
