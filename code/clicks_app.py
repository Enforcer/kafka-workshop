import faust
import logging


app = faust.App(
    "clicks_app",
    broker="kafka://broker:9092",
    processing_guarantee="exactly_once",
)


class Click(faust.Record):
    url: str


clicks = app.topic(
    "clicks",
    value_type=Click,
    partitions=4,
)

clicks_summary = app.Table(
    "clicks_summary",
    default=lambda: 0,
    partitions=4,
)


@app.agent(clicks)
async def process_clicks(stream):
    async for click in stream:
        clicks_summary[click.url] += 1


@app.page("/clicks/{url}/")
@app.table_route(table=clicks_summary, match_info="url")
async def get_clicks(web, request, url):
    return web.json(
        {
            url: clicks_summary[url],
        }
    )


@app.crontab("* * * * *", on_leader=False)
async def every_minute():
    logging.info("Running crontab!")
    logging.info(clicks_summary.items())


@app.timer(interval=5.0)
async def every_5_seconds():
    logging.info(clicks_summary.items())


@app.command()
async def create_topics():
    await clicks.maybe_declare()


@app.command()
async def generate_clicks():
    for url in ("google.com", "bottega.com.pl", "example.com"):
        for _ in range(3):
            await clicks.send(value=Click(url=url), key=url)


if __name__ == "__main__":
    app.main()
