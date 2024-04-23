from faust import App, Record
import logging


app = App(
    id="example_faust_app",
    broker="kafka://broker:9092",
)


class OrderLine(Record):
    sku: str
    unit_price: int
    quantity: int
    customer_id: int


orders_lines = app.topic(
    "orders_lines",
    key_type=str,
    value_type=OrderLine,
    partitions=4,
)


@app.agent(orders_lines)
async def process_orders_lines(stream):
    async for order_line in stream:
        logging.info("Got order line: %r", order_line)


@app.command()
async def create_topics():
    await orders_lines.maybe_declare()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.main()
