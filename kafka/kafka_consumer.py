from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "foobar", auto_offset_reset="earliest", bootstrap_servers=["localhost:9092"]
)
for msg in consumer:
    print(msg)
