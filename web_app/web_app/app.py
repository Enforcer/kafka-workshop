import asyncio
import json
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from confluent_kafka import Producer
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

from web_app.tracing import setup_tracer
from web_app.database import OutboxEntry, db_session, User, engine

config = {
    "bootstrap.servers": "broker:9092",
    "acks": "all",
    "message.send.max.retries": 1,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    poll_task = asyncio.create_task(producer_poll())
    yield
    poll_task.cancel()


app = FastAPI(lifespan=lifespan)
setup_tracer(service_name="web-app", app=app, engine=engine)

producer = ConfluentKafkaInstrumentor.instrument_producer(Producer(config))


async def producer_poll():
    while True:
        producer.poll(0)
        await asyncio.sleep(1)


def produce_callback(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")


@app.get("/")
def hello():
    return {"Hello": "world"}


@app.get("/message")
def send_message() -> Response:
    value = json.dumps({"message": "Hello, world!", "random": secrets.token_hex(4)})
    producer.produce(
        topic="web-app-producer",
        value=value,
        on_delivery=produce_callback,
    )
    return Response(status_code=202, content="Message scheduled to be send")


@app.get("/outbox")
def outbox() -> dict:
    with db_session() as session:
        username = secrets.token_hex(4)

        new_user = User(username=username)
        session.add(new_user)
        session.flush()

        payload = {
            "id": new_user.id,
            "username": new_user.username,
        }
        producer.produce(
            topic="users",
            value=json.dumps(payload).encode(),
            on_delivery=produce_callback,
        )

        session.commit()

    return {"result": "user created", "username": username}
