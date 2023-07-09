from datetime import date
import pandas as pd
from kafka_ultility import create_consumer
from constant import *


def main():
    consumer = create_consumer(TOPIC_NAME)
    for count, message in enumerate(consumer):
        today = str(date.today())
        df = pd.read_json(message.value)
        df.to_csv(
            f"s3://{S3_BUCKET_NAME}/{S3_OUTPUT_DIRECTORY}/stock_market_{today}.csv",
            index=False,
            storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY},
        )


if __name__ == "__main__":
    main()
