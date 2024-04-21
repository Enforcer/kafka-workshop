import json
from contextlib import contextmanager

from confluent_kafka import Consumer
from packaging.metadata import Metadata
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import as_declarative, Session, sessionmaker
from sqlalchemy.orm.scoping import scoped_session

TOPIC = "duplicates"
ENGINE = create_engine("postgresql+psycopg2://kafka:kafka@postgres:5432/kafka")
session_factory = sessionmaker(bind=ENGINE)
ScopedSession = scoped_session(session_factory)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "broker:9092",
            "group.id": "duplicates-handling-1",
            "group.instance.id": "singleton",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    while True:
        event = consumer.poll(1.0)
        if event is None:
            continue

        payload = json.loads(event.value())
        print(f"Got message {payload}")
        with db_session() as session:
            job_alert_subscription = JobAlertSubscription(
                employment_type=payload["employment_type"],
                job_location=payload["job_location"],
                remote=payload["remote"],
                keywords=payload["keywords"],
                availability=payload["availability"],
                email_address=payload["email_address"],
                contact_consent=payload["contact_consent"],
            )
            session.add(job_alert_subscription)
            session.commit()
        print(f"Saved JobAlertSubscription for {payload['email_address']}")


@as_declarative()
class Base:
    metadata: Metadata


class MessagesIds(Base):
    __tablename__ = "messages_ids"

    id = Column(Integer(), primary_key=True)


class JobAlertSubscription(Base):
    __tablename__ = "job_alert_subscriptions"

    id = Column(Integer(), primary_key=True)
    employment_type = Column(String(255), nullable=False)
    job_location = Column(String(255), nullable=True)
    remote = Column(Boolean(), nullable=False)
    keywords = Column(JSONB(), nullable=False, server_default="[]")
    availability = Column(String(255), nullable=False)
    email_address = Column(String(255), nullable=False, unique=True)
    contact_consent = Column(Boolean(), nullable=False)
    version = Column(Integer(), nullable=False, server_default="1")


@contextmanager
def db_session() -> Session:
    session = ScopedSession()
    try:
        yield session
    except Exception:
        raise
    finally:
        ScopedSession.remove()


if __name__ == "__main__":
    Base.metadata.create_all(bind=ENGINE)
    main()
