import json
import logging
import random
import sys

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient


TOPIC = "orders_lines"
BOOTSTRAP_SERVERS = "broker:9092"


def generate() -> dict:
    skus = ["WE-70751", "BW-95665", "SF-79924", "TU-15084", "NY-12102"]
    unit_prices = [2, 5, 1, 29, 99]
    skus_unit_prices = dict(zip(skus, unit_prices))
    customers_ids = [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008]

    sku = random.choice(skus)
    return {
        "sku": sku,
        "customer_id": random.choice(customers_ids),
        "unit_price": skus_unit_prices[sku],
        "quantity": random.randint(1, 10),
    }


def main() -> None:
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "allow.auto.create.topics": False,
            "acks": "all",
            "message.send.max.retries": 0,
        }
    )
    for _ in range(10):
        payload = generate()
        value = json.dumps(payload)
        key = payload["sku"]
        producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
    producer.flush()


def on_delivery(err, msg):
    if err is not None:
        logging.error("Failed to deliver message: %s", err)
    else:
        logging.info(
            "Message delivered to %s [%s] at offset {%d}",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def ensure_topic_exists() -> None:
    client = AdminClient(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
        }
    )
    topic_metadata = client.list_topics()
    if TOPIC not in topic_metadata.topics:
        logging.error("Topic %s does not exist, create it first in Faust.", TOPIC)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ensure_topic_exists()
    main()
