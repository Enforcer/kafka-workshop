import json
from threading import Event

from confluent_kafka import Producer, Consumer


class Prosumer:
    stop_event: Event

    def __init__(
        self, bootstrap_servers: str, group_id: str, in_topic: str, out_topic: str
    ) -> None:
        self._producer_config = {"bootstrap.servers": bootstrap_servers}
        self._consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
        }
        self._in_topic = in_topic
        self._out_topic = out_topic
        self.stop_event = Event()

    def run(self) -> None:
        producer = Producer(self._producer_config)
        consumer = Consumer(self._consumer_config)

        consumer.subscribe([self._in_topic])
        try:
            while True:
                if self.stop_event.is_set():
                    break
                event = consumer.poll(1.0)
                if event is None:
                    continue

                value = json.dumps(json.loads(event.value()))
                producer.produce(
                    topic=event.topic(), value=value.encode(), key=event.key()
                )
                producer.flush()
        finally:
            consumer.close()


if __name__ == "__main__":
    prosumer = Prosumer(
        bootstrap_servers="broker:9092",
        in_topic="in-topic",
        out_topic="out-topic",
    )
    prosumer.run()
