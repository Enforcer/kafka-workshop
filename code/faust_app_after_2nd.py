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

price_summary_by_sku = app.Table(
    "price_summary_by_sku",
    default=int,
    partitions=4,
)


@app.agent(orders_lines)
async def process_orders_lines(stream):
    async for order_line in stream:
        logging.info("Got order line: %r", order_line)
        price_summary_by_sku[order_line.sku] += (
            order_line.unit_price * order_line.quantity
        )


@app.page("/summary/{sku}/")
@app.table_route(table=price_summary_by_sku, match_info="sku")
async def get_summary(web, request, sku):
    return web.json(
        {
            sku: price_summary_by_sku[sku],
        }
    )


@app.command()
async def create_topics():
    await orders_lines.maybe_declare()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.main()
