from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
from json import dumps, loads
from constant import *


def create_admin_client(client_id=CLIENT_ID):
    return KafkaAdminClient(
        bootstrap_servers=f"{IP_ADDRESS}:{PORT}", client_id=client_id
    )


def create_topics(topic_names: str, admin_client: KafkaAdminClient):
    try:
        topic = NewTopic(name=topic_names, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print("Topic Created Successfully")
    except Exception as e:
        print(e)


def delete_topics(topic_names: str, admin_client: KafkaAdminClient):
    try:
        admin_client.delete_topics(topics=topic_names)
        print("Topic Deleted Successfully")
    except Exception as e:
        print(e)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[f"{IP_ADDRESS}:{PORT}"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )


def create_consumer(topic_name):
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=[f"{IP_ADDRESS}:{PORT}"],
        value_deserializer=lambda x: loads(x.decode("utf-8")),
    )
