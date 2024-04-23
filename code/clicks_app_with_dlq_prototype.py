import uuid
from datetime import datetime, timedelta, timezone

import faust
import logging


app = faust.App(
    "clicks_app",
    broker="kafka://broker:9092",
    # processing_guarantee="exactly_once",
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


class DeadLetterQueueEntry(faust.Record):
    id: str
    data: Click
    next_retry_at: datetime
    retries_left: int


dead_letter_queue = app.topic(
    "dead_letter_queue",
    value_type=DeadLetterQueueEntry,
    partitions=4,
)

dead_letter_queue_table = app.Table(
    "dead_letter_queue_table",
    value_type=DeadLetterQueueEntry,
    partitions=4,
)


dead_letter_retries = app.topic(
    "dead_letter_queue_retries",
    value_type=DeadLetterQueueEntry,
    partitions=4,
)


@app.agent(clicks)
async def process_clicks(stream):
    index = 1
    async for click in stream:
        index += 1
        if index % 3 == 0:
            entry = DeadLetterQueueEntry(
                id=uuid.uuid4().hex,
                data=click,
                next_retry_at=datetime.now(tz=timezone.utc) + timedelta(seconds=5),
                retries_left=3,
            )
            logging.info("Going to dead-letter!")
            await dead_letter_queue.send(value=entry)
        else:
            logging.info("Handling ok")
            clicks_summary[click.url] += 1


@app.agent(dead_letter_queue)
async def register_dlq_entries(stream):
    async for entry in stream:
        dead_letter_queue_table[entry.id] = entry


@app.agent(dead_letter_retries)
async def retry_dead_letters(stream):
    async for entry in stream:
        logging.info(
            f"[retrying] Retrying entry {entry.id}, retries left: {entry.retries_left}"
        )
        if entry.retries_left > 0:
            logging.info(
                f"[retrying] Lowering entries left for {entry.id}, old: {entry.retries_left}"
            )
            entry.retries_left -= 1
            entry.next_retry_at = datetime.now(tz=timezone.utc) + timedelta(seconds=15)
            dead_letter_queue_table[entry.id] = entry  # must explicitly assign
            # trigger logic (shouldn't republish to the original topic!)
        else:
            logging.info(f"Discarding entry {entry.id}")
            del dead_letter_queue_table[entry.id]


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
    for entry in dead_letter_queue_table.values():
        retry_at = (
            entry.next_retry_at
            if isinstance(entry.next_retry_at, datetime)
            else datetime.fromisoformat(entry.next_retry_at).replace(
                tzinfo=timezone.utc
            )
        )
        if retry_at > datetime.now(tz=timezone.utc):
            continue

        # can't modify table inside this loop, need to be consuming stream
        # so we're using extra topic to trigger retry logic when the time has come
        logging.info(
            f"[timer] Retrying entry {entry.id}, retries left: {entry.retries_left}"
        )
        await dead_letter_retries.send(value=entry, key=entry.data.url)


@app.command()
async def create_topics():
    await clicks.maybe_declare()
    await dead_letter_queue.maybe_declare()
    await dead_letter_retries.maybe_declare()


@app.command()
async def generate_clicks():
    for url in ("google.com", "bottega.com.pl", "example.com"):
        for _ in range(1):
            await clicks.send(value=Click(url=url), key=url)


if __name__ == "__main__":
    app.main()
