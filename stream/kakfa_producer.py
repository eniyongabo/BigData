import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
TOPIC = "electronic_store"

with open("events.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        msg = json.dumps(row)
        producer.send(TOPIC, bytes(msg, "utf-8"))
        print(msg)
        time.sleep(0.5)
