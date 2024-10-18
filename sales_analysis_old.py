import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator
import sys
import os
from operators.generate_data import generate_data
from operators.clickhouse_transfer import clickhouse_transfer

dag = DAG(
    dag_id = "sales_analysis",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval='45 9 * * 2',
)

create_pg_tables = PostgresOperator(
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

        DROP TABLE IF EXISTS sales;
        CREATE table sales (
            sale_id INT PRIMARY KEY,
            customer_id INT,
            product_id INT,
            quantity INT,
            sale_date DATE,
            sale_amount INT,
            region VARCHAR(5)
        );
    ''',
    postgres_conn_id='postgres_conn_id',
    dag=dag
)

task_generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    op_kwargs= {'num_sales': 1000000, 'num_products': 10},
    dag=dag
)

task_clear_data = SparkSubmitOperator(
    task_id="clear_data",
    conn_id="spark-conn",
    application="plugins/operators/clear_data.py",
    dag=dag
)

aggr_funcs = PostgresOperator(
    task_id='aggr_funcs',
    sql='''
        DROP TABLE IF EXISTS aggr_table;
        CREATE TABLE aggr_table AS 
        SELECT product_id, region, count(sale_id) as count_sales, sum(sale_amount) as sum_sales, ROUND(AVG(sale_amount)::numeric, 1) AS average_sale_amount 
        FROM sales GROUP BY region, product_id;
    ''',
    postgres_conn_id='postgres_conn_id',
    dag=dag
)

clickhouse_transfer = PythonOperator(
    task_id='clickhouse_transfer',
    python_callable=clickhouse_transfer,
    dag=dag,
)

create_pg_tables >> task_generate_data >> task_clear_data >> aggr_funcs >> clickhouse_transfer
