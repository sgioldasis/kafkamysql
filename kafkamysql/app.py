import os
from time import sleep
from confluent_kafka import Consumer, KafkaError
import json
import logging

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
KAFKA_TOPIC = 'data'

settings = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'group.id': 'my-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

class KafkaMySql:

    @staticmethod
    def run():
        logging.warning('KAFKA_BROKER_URL = '+KAFKA_BROKER_URL)

        consumer = Consumer(settings)
        consumer.subscribe([KAFKA_TOPIC])

        try:
            while True:
                msg = consumer.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    msg_data = msg.value().decode('utf-8')
                    print('msg_data=',msg_data)
                    logging.warning('msg_data='+msg_data)
                    if msg_data == 'Quit!':
                        break
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning('End of partition reached {0}/{1}'
                        .format(msg.topic(), msg.partition()))
                else:
                    logging.warning('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            consumer.close()
