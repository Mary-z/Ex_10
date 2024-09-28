import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def clear_data():
    command = [
        'psql',
        '-h', 'postgres_user',
        '-U', 'user',
        '-d', 'test',
        '-c', 'DROP TABLE IF EXISTS sales; \
                  CREATE table sales ( \
                  sale_id INT PRIMARY KEY, \
                  customer_id INT, \
                  product_id INT, \
                  quantity INT, \
                  sale_date DATE, \
                  sale_amount INT, \
                  region VARCHAR(5) \
                  );'
    ]

    #spark = SparkSession.builder.config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar").getOrCreate()
    spark = SparkSession.builder.appName("My App").config("spark.driver.extraClassPath", "/opt/airflow/lib/postgresql-42.7.4.jar").config("spark.executor.extraClassPath", "/opt/airflow/lib/postgresql-42.7.4.jar").config("spark.jars", "/opt/airflow/lib/postgresql-42.7.4.jar").getOrCreate()
    #spark = SparkSession.builder.appName("My App").config("spark.jars", "/opt/airflow/lib/postgresql-42.7.4.jar").getOrCreate()
    #spark = SparkSession.builder.appName("AppName").config("spark.jars.packages", "org.postgresql:postgresql:42.7.4").getOrCreate()
    #spark.sparkContext.addJar("/opt/airflow/lib/postgresql-42.7.4.jar")
    #spark.conf.set("spark.jars", "file:///opt/bitnami/spark/jars/postgresql-42.7.4.jar")

    jdbc_url = "jdbc:postgresql://postgres_user:5432/test"
    db_username = "user"
    db_password = "password"
    table_name = "sales_temp"

    options = {
        "url": jdbc_url,
        "dbtable": table_name,
        "user": db_username,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    sales_df = spark.read.format("jdbc").options(**options).load()

    sales_df_unique = sales_df.dropDuplicates() # Удаление дубликатов

    spark.stop()

clear_data()