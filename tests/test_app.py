import os
from .context import kafkamysql
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
import logging


KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC = 'data'

def example_create_topics(a, topics):
    """ Create topics """

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
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

def test_app(capsys, example_fixture):
    # Create Admin client
    a = AdminClient({'bootstrap.servers': KAFKA_BROKER_URL})
    example_create_topics(a, [KAFKA_TOPIC])

    producer = Producer({'bootstrap.servers': KAFKA_BROKER_URL})
    producer.produce(KAFKA_TOPIC, key='1', value='Hello, World!')
    producer.produce(KAFKA_TOPIC, key='2', value='This is Kafka-Python')
    producer.produce(KAFKA_TOPIC, key='2', value='Quit!')
    producer.flush(10)

    # pylint: disable=W0612,W0613
    kafkamysql.KafkaMySql.run()
    captured = capsys.readouterr()

    assert "Hello, World" in captured.out
    assert "Kafka-Python" in captured.out
