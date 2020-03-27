import logging
import pytest
import os
from .context import utils
from confluent_kafka import Producer
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    NewPartitions,
    ConfigResource,
    ConfigSource,
)

LOGGER = logging.getLogger(__name__)


def kafka_create_topics(a, topics):
    """ Create topics """

    new_topics = [
        NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics
    ]
    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(new_topics)

    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0
    # to the create_topics() call.
    # All futures will finish at the same time.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))


def kafka_delete_topics(a, topics):
    """ delete topics """

    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))


@pytest.fixture(scope="session")
def env():
    return os.getenv("TEST_ENV")


@pytest.fixture(scope="session")
def config(env):
    return utils.load_config(env)


@pytest.fixture(scope="session")
def db_connection(config):
    mysql_config = config["mysql"]
    mysql_table = mysql_config["table"]
    connection = utils.connect(mysql_config)
    yield connection
    connection.close()


@pytest.fixture(scope="function")
def mysql_table(config, db_connection):
    db_cursor = db_connection.cursor()
    db_cursor.execute(f"delete from `{config['mysql']['table']}`")
    db_connection.commit()
    db_cursor.close()
    return config["mysql"]["table"]


@pytest.fixture(scope="session")
def kafka_admin_client(config):
    return AdminClient({"bootstrap.servers": config["kafka"]["broker_url"]})


@pytest.fixture(scope="session")
def kafka_producer(config):
    return Producer({"bootstrap.servers": config["kafka"]["broker_url"]})


@pytest.fixture(scope="function")
def kafka_topic(config, kafka_admin_client):
    kafka_topic = config["kafka"]["topic"]
    kafka_create_topics(kafka_admin_client, [kafka_topic])
    yield kafka_topic
    kafka_delete_topics(kafka_admin_client, [kafka_topic])
