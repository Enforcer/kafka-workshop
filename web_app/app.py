import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from confluent_kafka import Producer


config = {
    "bootstrap.servers": "broker:9092",
    "acks": "all",
    "message.send.max.retries": 1
}
producer = Producer(config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    poll_task = asyncio.create_task(producer_poll())
    yield
    poll_task.cancel()


app = FastAPI(lifespan=lifespan)


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
    producer.produce(topic="web-app-producer", value=b"{}", on_delivery=produce_callback)
    return Response(status_code=202, content="Message scheduled to be send")
