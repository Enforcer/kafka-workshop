import socket
import random

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField

from pydantic import BaseModel


TOPIC = "messages-with-a-schema"
MESSAGES_COUNT = 1
SCHEMA_REGISTRY_CONFIG = {"url": "http://schema_registry:8081/"}
PRODUCER_CONFIG = {
    "bootstrap.servers": "broker:9092",
    "client.id": socket.gethostname(),
}


schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
producer = Producer(PRODUCER_CONFIG)


class Payload(BaseModel):
    pass  # TODO


json_serializer = JSONSerializer(
    Payload.schema_json(),
    schema_registry_client,
    lambda model, _ctx: model.model_dump(mode="json"),
)


def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Message {event.value().decode("utf8")} produced to {event.topic()}')


def main() -> None:
    data = [Payload(**generate()) for _ in range(MESSAGES_COUNT)]

    for piece_of_data in data:
        serialized_value = json_serializer(
            piece_of_data,
            SerializationContext(TOPIC, MessageField.VALUE),
        )

        producer.produce(
            topic=TOPIC,
            value=serialized_value,
            on_delivery=delivery_report,
        )

    producer.flush()


def generate() -> dict:
    keywords_count = random.randint(1, 3)
    all_keywords = [
        "python",
        "kafka",
        "flink",
        "spark",
        "sql",
        "django",
        "fastapi",
        "scrum",
        "agile",
        "kanban",
    ]
    random.shuffle(all_keywords)

    return {
        "employment_type": random.choice(["part_time", "full_time"]),
        "job_location": random.choice(["Warsaw", "Berlin", "Paris"] + [None] * 3),
        "remote": random.choice([True, False]),
        "keywords": [all_keywords.pop() for _ in range(keywords_count)],
        "availability": random.choice(["immediately", "in_1_month", "in_3_months"]),
        "email_address": f"test+{random.randint(1, 10_000)}@test.pl",
        "contact_consent": True,
    }


def create_topic() -> None:
    admin = AdminClient({"bootstrap.servers": PRODUCER_CONFIG["bootstrap.servers"]})
    metadata = admin.list_topics()
    if TOPIC in metadata.topics.keys():
        return
    new_topic = NewTopic(topic=TOPIC, num_partitions=4, replication_factor=1)
    futures = admin.create_topics([new_topic])
    futures[TOPIC].result()


if __name__ == "__main__":
    create_topic()
    main()
