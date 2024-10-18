import psycopg2
from datetime import datetime
from clickhouse_driver import Client
import os

def clickhouse_transfer():
  connection = psycopg2.connect(
    host=os.getenv('PG_HOST'),
    database=os.getenv('PG_NAME'),
    user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD')
  )
  cursor = connection.cursor()
  cursor.execute("SELECT * FROM aggr_table;")
  rows = cursor.fetchall()
  cursor.close()
  connection.close()
  current_date = datetime.now().date()
  rows_with_date = [row + (current_date,) for row in rows]
  ch_client = Client(host=os.getenv('CH_HOST'),
                     user=os.getenv('CH_USER'),           
                     password=os.getenv('CH_PASSWORD'))
  ch_client.execute('CREATE TABLE IF NOT EXISTS sales_aggr (product_id Int32, region String, count_sales Int32, sum_sales Int32, average_sale_amount Float64, insert_date date) ENGINE = MergeTree() ORDER BY insert_date')
    
  ch_client.execute('INSERT INTO sales_aggr VALUES', rows_with_date)
