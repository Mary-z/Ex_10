import os
import logging
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import psycopg2
import pandas as pd
from pyspark.sql.functions import when, mean
logging.basicConfig(level=logging.INFO)
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from airflow.providers.postgres.hooks.postgres import PostgresHook

def clear_data():
    logger = logging.getLogger("airflow.task")
    connection = psycopg2.connect(
        host=os.getenv('PG_HOST'), 
        database=os.getenv('PG_NAME'), 
        user=os.getenv('PG_USER'), 
        password=os.getenv('PG_PASSWORD')
    )
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM sales_temp;")
    spark = SparkSession.builder.appName('MyApp').getOrCreate()
    schema = StructType([
    StructField("sale_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("sale_date", DateType(), nullable=True),
    StructField("sale_amount", IntegerType(), nullable=True),
    StructField("region", StringType(), nullable=True)
    ])
    sales_df = spark.createDataFrame(cursor.fetchall(), schema)
    sales_df_unique = sales_df.dropDuplicates() # Удаление дубликатов
    sales_df_unique.persist()
    # Вычисление средней стоимости продукта исключая выбросы
    mean_price = sales_df_unique \
    .select((F.col('sale_amount') / F.col('quantity')).alias('price_per_unit')) \
    .filter(F.col('price_per_unit') <= 1000) \
    .agg(F.avg('price_per_unit').alias('mean_price')) \
    .first()[0]
    sales_df_unique = sales_df_unique.withColumn("sale_amount",
                                                 # Замена слишком больших значений sale_amount средними
                                                 when((sales_df_unique["sale_amount"] / sales_df_unique[
                                                     "quantity"]) > 1000,
                                                      mean_price * sales_df_unique["quantity"]).otherwise(
                                                     sales_df_unique["sale_amount"])
                                                 )
    sales_df_unique = sales_df_unique.withColumn("sale_amount", F.round(sales_df_unique["sale_amount"]))
    # Приведение стоимостей покупок к целому числу, замена на среднее по столбцу при невозможности
    sales_df_unique = sales_df_unique.withColumn(
    'sale_amount',
    F.when(
        (sales_df_unique['sale_amount'].cast(IntegerType()).isNull()) | 
        (F.isnan(sales_df_unique['sale_amount'].cast(IntegerType()))),
        mean_price
    ).otherwise(
        sales_df_unique['sale_amount'].cast(IntegerType())
    )
    )
    
    # Записываем DataFrame в PostgreSQL порциями по 100000 строк
    df_count = sales_df_unique.count()
    batch_size = 100000
    # Конвертируем DataFrame в список кортежей для упрощения вставки
    data_to_insert = sales_df_unique.collect()
    
    for i in range(0, df_count, batch_size):
        batch_data = data_to_insert[i:i + batch_size]
    
        # Создаем строку вставки
        insert_query = "INSERT INTO sales (sale_id, customer_id, product_id, quantity, sale_date, sale_amount, region) VALUES %s"
        # Формируем значения для вставки
        values_str = ', '.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s)", row).decode("utf-8") for row in batch_data)
        # Выполняем вставку
        cursor.execute(insert_query % values_str)
        logger.info(f'Inserted {i} rows')
        
    connection.commit()
    cursor.close()
    connection.close()
    spark.stop()

if __name__ == "__main__":
  clear_data()
