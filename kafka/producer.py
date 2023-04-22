from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import csv
import json
import time

producer = Producer({"bootstrap.servers": "localhost:9092"})
TOPIC = "electronic-store"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def create_topics():
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    topic_list = [NewTopic(x) for x in [TOPIC, "electronic-analytics"]]
    admin_client.create_topics(topic_list)


create_topics()


with open("kafka/dataset.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        producer.poll(0)
        msg = json.dumps(row)
        producer.produce(TOPIC, msg.encode("utf-8"), callback=delivery_report)
        time.sleep(0.05)
