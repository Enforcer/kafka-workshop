"""Treat as pseudocode"""
import functools
import json
import logging
import time

from confluent_kafka import Producer

from web_app.database import db_session, OutboxEntry


CONFIG = {
    "bootstrap.servers": "broker:9092",
    "acks": "all",
    "message.send.max.retries": 0,
    "transactional.id": "outbox_processor_web_app",
}


def run_once(producer: Producer) -> None:
    with db_session() as session:
        entries = (
            session.query(OutboxEntry)
            .with_for_update(skip_locked=True)
            .filter(OutboxEntry.retries_left >= 0)
            .order_by(OutboxEntry.when_created)
            .limit(100)
            .all()
        )

        def on_delivery(session, entry, error, _message):
            if error is None:
                session.delete(entry)
            else:
                entry.retries_left -= 1

        producer.begin_transaction()
        for entry in entries:
            producer.produce(
                topic=entry.topic,
                value=json.dumps(entry.data),
                on_delivery=functools.partial(on_delivery, session, entry),
            )

        producer.commit_transaction()
        # + error handling
        session.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    producer = Producer(CONFIG)
    producer.init_transactions()

    logging.info("Running Outbox Processor...")
    while True:
        run_once(producer)
        time.sleep(1)


if __name__ == "__main__":
    main()
