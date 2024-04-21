import functools
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

TOPIC = "messages-with-a-schema"
CONSUMER_CONFIG = {
    "bootstrap.servers": "broker:9092",
    "group.id": "schema-aware-consumers",
    "auto.offset.reset": "earliest",
}
THREADS = 2
SCHEMA_REGISTRY_CONFIG = {"url": "http://schema_registry:8081/"}


def log_exception_and_retry(fun):
    @functools.wraps(fun)
    def wrapped(*args, **kwargs):
        last_fail = 0
        while True:
            try:
                fun(*args, **kwargs)
            except Exception:
                logging.exception("Ouch, something's wrong!")
                now = time.time()
                if now - last_fail < 5:
                    time.sleep(now - last_fail)
                last_fail = now
            else:
                break

    return wrapped


@log_exception_and_retry
def consume(topic: str, consumer_number: int, stop_event: Event) -> None:
    logging.info("Starting Consumer #%d", consumer_number)
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    consumer = Consumer(
        {**CONSUMER_CONFIG, "group.instance.id": f"thread-{consumer_number}"}
    )
    consumer.subscribe([topic])

    while True:
        if stop_event.is_set():
            consumer.close()
            logging.info("Consumer #%d turning off", consumer_number)
            return

        event = consumer.poll(1.0)
        if event is None:
            continue

        logging.info("Consumer #%d got %r", consumer_number, event.value())
        # TODO
        # event.value()  # returns value of a record as bytes

        # schema_registry_client.get_latest_version(f"{TOPIC}-value")
        #   get_latest_version returns RegisteredSchema.
        #   To get Schema, use .schema attribute

        # Build JSONDeserializer
        # deserializer = JSONDeserializer(schema)

        # Run deserializer
        #   deserializer(value, SerializationContext(event.topic(), MessageField.VALUE))}


def main() -> None:
    stop_event = Event()
    thread_pool = ThreadPoolExecutor(max_workers=THREADS)
    for consumer_number in range(THREADS):
        thread_pool.submit(consume, TOPIC, consumer_number, stop_event)

    try:
        thread_pool.shutdown(wait=True)
    except KeyboardInterrupt:
        stop_event.set()

    try:
        thread_pool.shutdown(wait=True)
    except KeyboardInterrupt:
        logging.warning("Whoah, curb your CTRL+C dude! Shutdown in progress")

    # Make sure logs are printed out
    for handler in logging.getLogger().handlers:
        handler.flush()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s:%(levelname)s:%(message)s", level=logging.INFO
    )
    main()
