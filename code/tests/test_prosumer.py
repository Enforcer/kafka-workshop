import uuid

import pytest
from threading import Thread
from code.prosumer_for_testing import Prosumer


@pytest.fixture()
def in_topic() -> str:
    topic_name = "in"
    # create topic
    yield topic_name
    # remove topic


@pytest.fixture()
def out_topic() -> str:
    topic_name = "out"
    # create topic
    yield topic_name
    # remove topic


@pytest.fixture()
def group_id() -> str:
    return uuid.uuid4().hex


@pytest.fixture()
def run_prosumer(group_id: str, in_topic: str, out_topic: str) -> None:
    prosumer = Prosumer(
        bootstrap_servers="broker:9092",
        group_id=group_id,
        in_topic=in_topic,
        out_topic=out_topic,
    )
    thread = Thread(target=prosumer.run)
    thread.start()
    yield
    prosumer.stop_event.set()
    thread.join()


@pytest.mark.usefixtures("run_prosumer")
def test_echoes_messages() -> None:
    # how to "trigger" prosumer's logic?
    # how to check if it has done its job?
    pass
