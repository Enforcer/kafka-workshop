import faust
from faust.sensors.prometheus import setup_prometheus_sensors
import logging

logger = logging.getLogger()

app = faust.App('myapp', broker='kafka://broker:9092')

setup_prometheus_sensors(app)

counts = app.Table('url_clicks_summary', default=int, partitions=1)

click_topic = app.topic('url_clicks', key_type=str, value_type=int, partitions=1)


@app.command()
async def create_topics():
    await click_topic.maybe_declare()
    await counts.changelog_topic.maybe_declare()


@app.agent(click_topic)
async def count_click(clicks):
    await click_topic.send(key="google.com", value=1)
    async for url, count in clicks.items():
        counts[url] += count
        logger.info("%s has now %d clicks!", url, counts[url])


if __name__ == '__main__':
    app.main()

