import logging
import time

from web_app.database import db_session, OutboxEntry


def run_once():
    with db_session() as session:
        pass


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    logging.info("Running Outbox Processor...")
    while True:
        run_once()
        time.sleep(1)


if __name__ == "__main__":
    main()
