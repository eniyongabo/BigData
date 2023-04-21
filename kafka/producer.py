from confluent_kafka import Producer
import csv
import json
import time

producer = Producer({"bootstrap.servers": "localhost:9092"})
TOPIC = "electronic_store"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


with open("kafka/dataset.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        producer.poll(0)
        msg = json.dumps(row)
        producer.produce(TOPIC, msg.encode("utf-8"), callback=delivery_report)
        time.sleep(0.5)
