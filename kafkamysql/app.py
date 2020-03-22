import os
from time import sleep
from kafka import KafkaConsumer
import json
import logging

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC = 'data'

class KafkaMySql:

    @staticmethod
    def run():
        print('KAFKA_BROKER_URL =',KAFKA_BROKER_URL)
        logging.error('KAFKA_BROKER_URL = '+KAFKA_BROKER_URL)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER_URL],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: x.decode('utf-8'))

        for message in consumer:
            msg_data = message.value
            print('msg_data=',msg_data)
            logging.warning('msg_data='+msg_data)

            if msg_data == 'Quit!':
                break