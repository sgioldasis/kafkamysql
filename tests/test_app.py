import os
from .context import kafkamysql
from confluent_kafka import Producer
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    NewPartitions,
    ConfigResource,
    ConfigSource,
)
import logging
import json


KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")
KAFKA_TOPIC = "data"


def example_create_topics(a, topics):
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


def example_data():
    data = {
        "id": "d6707ce40a1447baaf012f948fb5b356",
        "customer_id": "8e0fdcefc3ad45acbb5a2abc506c6c9f",
        "created_at": "2019-05-04T23:34:19.5934998Z",
        "text": "<p>aliquam sed amet dolore consectetuer ut dolore elit dolor euismod elit erat</p>",
        "ad_type": "Premium",
        "price": 29.04,
        "currency": "EUR",
        "payment_type": "Card",
        "payment_cost": 0.17,
    }
    json_str = json.dumps(data)
    return json_str


def test_app(capsys, example_fixture):

    # Create Admin client
    a = AdminClient({"bootstrap.servers": KAFKA_BROKER_URL})
    example_create_topics(a, [KAFKA_TOPIC])

    producer = Producer({"bootstrap.servers": KAFKA_BROKER_URL})
    producer.produce(KAFKA_TOPIC, key="1", value="Hello, World!")
    producer.produce(KAFKA_TOPIC, key="2", value=example_data())
    producer.produce(KAFKA_TOPIC, key="3", value="Quit!")
    producer.flush(10)

    # pylint: disable=W0612,W0613
    kafkamysql.KafkaMySql.run()
    captured = capsys.readouterr()

    assert "Hello, World" in captured.out
    assert "d6707ce40a1447baaf012f948fb5b356" in captured.out
