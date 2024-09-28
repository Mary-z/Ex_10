from datetime import datetime
from faker import Faker
import random
import psycopg2

fake = Faker()

def generate_data(**kwargs):
    conn = psycopg2.connect(
        host='postgres_user',
        database='test',
        user='user',
        password='password'
    )
    cur = conn.cursor()
    sales_data = []
    insert_count = 0
    products = {}
    for _ in range(kwargs['num_products']):
        product_id = _ + 1 # Уникальный идентификатор продукта
        product_price = random.expovariate(0.004) # Стоимость продукта
        product_price = product_price if product_price > 10 else product_price + 10
        products[product_id] = product_price

    for _ in range(kwargs['num_sales']):
        sale_id = _ + 1  # Уникальный идентификатор продажи
        customer_id = random.randint(1, 100000)  # Идентификатор клиента
        product_id = random.choice(list(products.keys()))  # Идентификатор продукта
        temp_quantity = round(random.expovariate(0.6))
        quantity = temp_quantity if temp_quantity != 0 else temp_quantity + 1 # Количество продуктов
        sale_date = fake.date_between_dates(datetime(2023, 1, 1), datetime(2024, 8, 1))  # Дата продажи
        sale_amount = products[product_id] * quantity #round(random.uniform(10.0, 500.0), 2)  # Сумма продажи
        region = random.choice(['North', 'South', 'East', 'West'])  # Регион

        sales_data.append({
            'sale_id': sale_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'sale_date': sale_date,
            'sale_amount': sale_amount,
            'region': region
        })

        insert_count += 1

        if insert_count % 10 == 0:
            table_name = 'sales_temp'
            columns = ['sale_id', 'customer_id', 'product_id', 'quantity', 'sale_date', 'sale_amount',
                       'region']
            cur.executemany(
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                [tuple(row.values()) for row in sales_data])
            conn.commit()
            sales_data = []
            print(f'Inserted {insert_count} records so far')
    cur.close()
    conn.close()

