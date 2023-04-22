from confluent_kafka import Consumer
import json


class EventListener:
    def __init__(self, on_new_event) -> None:
        self.c = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": "dashboardg",
                "auto.offset.reset": "earliest",
            }
        )
        self.on_new_event = on_new_event 
        self.c.subscribe(["electronic-analytics"])

    def consume(self):
        msg = self.c.poll(1.0)
        while msg is None:
            msg = self.c.poll(1.0)
        if msg is None:
            print("None Message")
            return
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            return

        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")

        print("Received message: {} ;; {}".format(key, value))

        if key == "event_type_agg":
            data = json.loads(value)
            if len(data) > 0:
                self.on_new_event(json.loads(value))
