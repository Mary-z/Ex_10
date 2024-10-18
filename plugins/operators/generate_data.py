from datetime import datetime
from faker import Faker
import random
import psycopg2
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

def generate_data(**kwargs):
    fake = Faker()
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn_id", schema="test")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    sales_data = []
    insert_count = 0
    products = {}
    columns = ['sale_id', 'customer_id', 'product_id', 'quantity', 'sale_date', 'sale_amount', 'region']
    for _ in range(kwargs['num_products']):
        product_id = _ + 1 # Уникальный идентификатор продукта
        product_price = max(10, random.expovariate(0.004)) # Стоимость продукта
        products[product_id] = product_price

    for _ in range(kwargs['num_sales']):
        sale_id = _ + 1  # Уникальный идентификатор продажи
        customer_id = random.randint(1, 100000)  # Идентификатор клиента
        product_id = random.choice(list(products.keys()))  # Идентификатор продукта
        quantity = max(1, round(random.expovariate(0.6)))
        sale_date = fake.date_between_dates(datetime(2023, 1, 1), datetime(2024, 8, 1))  # Дата продажи
        sale_amount = products[product_id] * quantity # Сумма продажи
        region = random.choice(['North', 'South', 'East', 'West'])  # Регион

        sales_data.append((sale_id, customer_id, product_id, quantity, sale_date, sale_amount, region))
        insert_count += 1
        if insert_count % 100000 == 0:
            cursor.executemany(
                f"INSERT INTO sales_temp ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                sales_data)
            connection.commit()
            sales_data.clear()
    cursor.close()
    connection.close()
