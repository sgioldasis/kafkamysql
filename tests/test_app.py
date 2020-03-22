import os
from .context import kafkamysql
from kafka import KafkaProducer

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')

def test_app(capsys, example_fixture):

    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)
    producer.send('data', b'Hello, World!')
    producer.send('data', key=b'message-two', value=b'This is Kafka-Python')
    producer.send('data', b'Quit!')

    # pylint: disable=W0612,W0613
    kafkamysql.KafkaMySql.run()
    captured = capsys.readouterr()

    assert "Hello, World" in captured.out
    assert "Kafka-Python" in captured.out
