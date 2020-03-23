import os
from time import sleep
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
KAFKA_TOPIC = "data"

settings = {
    "bootstrap.servers": KAFKA_BROKER_URL,
    "group.id": "my-group",
    "client.id": "client-1",
    "enable.auto.commit": True,
    "session.timeout.ms": 6000,
    "default.topic.config": {"auto.offset.reset": "smallest"},
}


class KafkaMySql:
    @staticmethod
    def process(a_string):
        try:
            a_json = json.loads(a_string)
            logging.info(a_json)
            # df = pd.DataFrame.from_dict(a_json, orient="index")
            df = pd.DataFrame.from_dict([a_json])
            # df = pd.read_json('['+a_string+']', orient='columns') 

            # df['calc'] = df['created_at'].values.astype(str)
            calc = df.apply(lambda row: pd.to_datetime(row.created_at), axis=1)
            df['created_at_ts'] = calc.values.astype('int64')

            # df['sqlts'] = df.apply(lambda row: row.created_at.value.dt.round('1U'), axis=1)
            # df['sqlts'] = df['created_at'].values.astype('datetime64[us]')
            # df['sqlts2'] = df['created_at'].dt.round('1U').values.astype('datetime64[us]')
            # df['sqlts3'] = df['created_at'].dt.round('1U').values.astype(str)

            logging.info(df)
            logging.info(df.dtypes)

            # logging.info(df['created_at'].astype(str).tolist())
            # logging.info(df['created_at'].dt.tolist())
            # logging.info(df['created_at'].view('int64').tolist())

            # logging.info(df['sqlts'].astype(str).tolist())
            # logging.info(df['sqlts2'].astype(str).tolist())
            # logging.info(df['sqlts3'].astype(str).tolist())

            # logging.info(df['created_at'].dt.round('1U').astype(str).tolist())
            # logging.info(df['created_at'].dt.round('1U').tolist())
            # logging.info(df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').tolist())

            # creating column list for insertion
            cols = "`,`".join([str(i) for i in df.columns.tolist()])
            logging.info(cols)

            # Insert DataFrame recrds one by one.
            for i,row in df.iterrows():
                sql = "INSERT INTO `Classifieds` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                logging.info(sql)
                logging.info(tuple(row))

        except:
            logging.warning(f"String {a_string} could not be converted to JSON")
            
        finally:
            return a_string

    @staticmethod
    def run():
        logging.info("KAFKA_BROKER_URL = " + KAFKA_BROKER_URL)

        consumer = Consumer(settings)
        consumer.subscribe([KAFKA_TOPIC])

        try:
            while True:
                msg = consumer.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    msg_data = msg.value().decode("utf-8")
                    print("process(msg_data) = ", KafkaMySql.process(msg_data))
                    logging.info("msg_data=" + msg_data)
                    if msg_data == "Quit!":
                        break
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning(
                        "End of partition reached {0}/{1}".format(
                            msg.topic(), msg.partition()
                        )
                    )
                else:
                    logging.warning("Error occured: {0}".format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            consumer.close()
