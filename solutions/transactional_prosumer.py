from confluent_kafka import Consumer, Producer, TopicPartition, Message

producer_by_partition: dict[int, Producer] = {}


consumer_config = {
    "bootstrap.servers": "broker:9092",
    "group.id": "exactly-once-semantics",
    "group.instance.id": "prosumer",
    "enable.auto.commit": False,
}
consumer = Consumer(consumer_config)
topic = "eos"


def on_assign(a_consumer: Consumer, partitions: list[TopicPartition]) -> None:
    for partition in partitions:
        transactional_id = f"prosumer-{partition.partition}"
        producer = create_producer(transactional_id)
        producer_by_partition[partition.partition] = producer
        producer.init_transactions()


def create_producer(transactional_id: str) -> Producer:
    producer_config = {
        "bootstrap.servers": "broker:9092",
        "transactional.id": transactional_id,
    }
    return Producer(producer_config)


consumer.subscribe([topic], on_assign=on_assign)

while True:
    maybe_message: Message | None = consumer.poll(timeout=1.0)
    if maybe_message is None:
        continue

    partition = maybe_message.partition()
    producer = producer_by_partition[partition]
    producer.begin_transaction()
    producer.produce(topic="eos-results", value=maybe_message.value())
    topic_partitions = [
        TopicPartition(
            topic=maybe_message.topic(),
            partition=maybe_message.partition()
        ),
    ]
    position = consumer.position(topic_partitions)
    producer.send_offsets_to_transaction(
        position,
        consumer.consumer_group_metadata(),
    )
    producer.commit_transaction()
