from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import get_daily_stock_data
import kafka_producer
import kafka_consumer


# pip install apache-airflow
# pip install pandas
# pip install boto3
# pip install --upgrade google-api-python-client
# pip install --upgrade google-auth-oauthlib google-auth-httplib2


# create dag
with DAG(
    dag_id="kafka_stock_data",
    schedule="@daily",
    start_date=datetime(year=2023, month=7, day=3),
    catchup=False,
    tags=["kafka_stock"],
) as dag:
    # get stock data
    get_data_stock = PythonOperator(
        task_id="get_data_stock", python_callable=get_daily_stock_data.main
    )

    # producer push data
    producer = PythonOperator(
        task_id="kafka_producer", python_callable=kafka_producer.main
    )

    # consumer get message and updload data into s3
    consumer = PythonOperator(
        task_id="kafka_consumer", python_callable=kafka_consumer.main
    )

    # queue run
    get_data_stock >> producer >> consumer
