from confluent_kafka import Consumer

TOPIC = "web-app-producer"
config = {
    "bootstrap.servers": "broker:9092",
    "group.id": "web-app-consumer",
    "auto.offset.reset": "earliest",
    "group.instance.id": "web-app-consumer-singleton",
}


def main() -> None:
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue

        print(f"Consumed message: {message.value().decode('utf-8')}")


if __name__ == "__main__":
    main()
