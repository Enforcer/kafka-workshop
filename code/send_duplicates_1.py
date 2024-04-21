import json
import random
import time

from confluent_kafka import Producer

TOPIC = "duplicates"


def main() -> None:
    producer = Producer({"bootstrap.servers": "broker:9092"})
    message_id = int(time.time())
    message = generate()
    for _ in range(random.randint(3, 6)):
        producer.produce(
            topic=TOPIC,
            value=json.dumps(message),
            key=message["email_address"],
            headers={"message_id": str(message_id)},
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


if __name__ == "__main__":
    main()
