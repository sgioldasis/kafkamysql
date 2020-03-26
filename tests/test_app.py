import os
from .context import kafkamysql
from .context import utils
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
import time

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

def example_data2():
    data = {
        "id": "e",
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

    # env = "test"
    env = os.getenv("TEST_ENV")
    config = utils.load_config(env)


    # Connect to database and get a cursor
    mysql_config = config["mysql"]
    db_connection = utils.connect(mysql_config)
    mysql_table = mysql_config["table"]

    id = "d6707ce40a1447baaf012f948fb5b356"
    db_cursor = db_connection.cursor()
    db_cursor.execute(f"delete from `{mysql_table}`")
    db_connection.commit()
    db_cursor.close()

    # Create Admin client
    kafka_broker_url = config["kafka"]["broker_url"]
    kafka_topic = config["kafka"]["topic"]
    a = AdminClient({"bootstrap.servers": kafka_broker_url})
    example_create_topics(a, [kafka_topic])

    producer = Producer({"bootstrap.servers": kafka_broker_url})
    producer.produce(KAFKA_TOPIC, key="1", value="Hello, World!")
    producer.produce(KAFKA_TOPIC, key="2", value=example_data())
    producer.produce(KAFKA_TOPIC, key="3", value=example_data2())
    producer.produce(KAFKA_TOPIC, key="4", value="Quit!")
    producer.flush(10)

    # pylint: disable=W0612,W0613
    kafkamysql.KafkaMySql.run(env)
    captured = capsys.readouterr()

    # assert "Hello, World" in captured.out
    # assert "d6707ce40a1447baaf012f948fb5b356" in captured.out

    # Check fields
    from decimal import Decimal
    import datetime
    db_cursor = db_connection.cursor()
    db_cursor.execute(f"select * from `{mysql_table}` where id='{id}'")
    result = db_cursor.fetchall()
    db_cursor.close()
    assert result == [
        (
            "d6707ce40a1447baaf012f948fb5b356",
            "8e0fdcefc3ad45acbb5a2abc506c6c9f",
            "2019-05-04T23:34:19.5934998Z",
            "<p>aliquam sed amet dolore consectetuer ut dolore elit dolor euismod elit erat</p>",
            "Premium",
            Decimal("29.04"),
            "EUR",
            "Card",
            Decimal("0.17"),
            datetime.datetime(2019, 5, 4, 23, 34, 19, 593499),
            800,
        )
    ]

    # Close the cursor
    db_cursor.close()

    # Close the connection to database
    db_connection.close()
