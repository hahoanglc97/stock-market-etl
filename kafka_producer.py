from kafka_ultility import create_producer
import pandas as pd
from constant import *


def main():
    producer = create_producer()
    data = pd.read_json(DAILY_FILE)
    data = data.to_json(orient="records")
    producer.send(TOPIC_NAME, value=data)


if __name__ == "__main__":
    main()
