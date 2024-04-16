from confluent_kafka import Producer


config = {
    "bootstrap.servers": "broker:9092",
    "transactional.id": "producer-001",
}


producer = Producer(config)

"""
.init_transactions()
.begin_transaction()
.commit_transaction()
.abort_transaction()
"""