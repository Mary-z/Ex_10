#!/usr/bin/env bash

airflow connections add "postgres_conn_id" \
  --conn-json '{
    "conn_type": "postgres",
    "description": "",
    "login": "user",
    "password": "password",
    "host": "postgres_user",
    "port": 5432,
    "schema": "test",
    "extra": ""
  }';

airflow connections add "spark-conn" \
  --conn-json '{
    "conn_type": "spark",
    "description": "",
    "login": "airflow",
    "password": "airflow",
    "host": "spark://spark-master",
    "port": 7077,
    "schema": "",
    "extra": "{\"deploy-mode\": \"client\", \"spark-binary\": \"spark-submit\"}"
  }';

airflow connections add "spark_postgres" \
  --conn-json '{
    "conn_type": "spark_jdbc",
    "description": "",
    "login": "user",
    "password": "password",
    "host": "postgres_user",
    "port": 5432,
    "schema": "test",
    "extra": ""
  }';

#airflow connections add "clickhouse_conn_id" \
#  --conn-json '{
#    "conn_type": "ClickHouse",
#    "description": "",
#    "login": "user",
#    "password": "password",
#    "host": "clickhouse_user",
#    "port": 9440,
#    "schema": "test",
#    "extra": ""
#  }';
