from kafka_ultility import *
from get_daily_stock_data import get_daily_stock_data
import os.path
from datetime import date, timedelta
from constant import *
import pandas as pd


if __name__ == "__main__":
    # create topic
    admin_client = create_admin_client()
    try:
        create_topics(TOPIC_NAME, admin_client)
    except Exception as e:
        print(e)
        delete_topics(TOPIC_NAME, admin_client)
        create_topics(TOPIC_NAME, admin_client)

    # get historical stock
    yesterday = str(date.today() + timedelta(days=-1))
    if os.path.isfile(HISTORY_FILE) is False:
        # get historical stock data for first time run
        get_daily_stock_data(HISTORY_DATE_START, yesterday, history_crawl=True)

    # producer send data
    producer = create_producer()
    data = pd.read_json(HISTORY_FILE)
    data = data.to_json(orient="records")
    producer.send(TOPIC_NAME, value=data)

    # Reciving data on consumer and pushing data on S3
    consumer = create_consumer(TOPIC_NAME)
    for count, message in enumerate(consumer):
        df = pd.read_json(message.value)
        df.to_csv(
            f"s3://{S3_BUCKET_NAME}/{S3_OUTPUT_DIRECTORY}/stock_market_{yesterday}.csv",
            index=False,
            storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY},
        )
