import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
i = 0
while True:
    i += 1
    msg = f"This is the {i}th message"
    producer.send("foobar", bytes(msg, "utf-8"))
    time.sleep(1)
    print(msg)
