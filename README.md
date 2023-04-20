Fist run:

```shell
docker-compose up -d
```

To run the streamer code:
    1. Run `pip3 install -r 'stream/requirement.txt'`
    2. Run `python3 'stream/kafka_producer.py'`

And then run  the `KafkaStream.java` file. Make sure to add this vmArgs:
`--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED`

