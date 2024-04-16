import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, scoped_session, Session, DeclarativeBase


engine = create_engine("postgresql+psycopg2://kafka:kafka@postgres:5432/kafka")
session_factory = sessionmaker(bind=engine)
ScopedSession = scoped_session(session_factory)


@contextmanager
def db_session() -> Session:
    session = ScopedSession()
    try:
        yield session
    except Exception:
        raise
    finally:
        ScopedSession.remove()


class Base(DeclarativeBase):
    pass


class OutboxEntry(Base):
    __tablename__ = "outbox_entries"

    id = Column(Integer(), primary_key=True)
    queue = Column(String(255), nullable=False)
    data = Column(JSONB(), nullable=False)
    retries_left = Column(Integer, nullable=False, default=3)
    when_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


try:
    Base.metadata.create_all(bind=engine)
except Exception:
    logging.getLogger().warning("Failed to initialize database")
