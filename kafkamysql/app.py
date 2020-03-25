import os
from time import sleep
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd
from . import utils


# Main application class
class KafkaMySql:
    @staticmethod
    def init(env):
        # Load configuration
        config = utils.load_config(env)

        # Connect to Kafka and subscribe to topic
        KAFKA_BROKER_URL = config["kafka"]["broker_url"]
        KAFKA_TOPIC = config["kafka"]["topic"]
        KAFKA_SETTINGS = config["kafka"]["settings"]

        logging.info("KAFKA_BROKER_URL: " + KAFKA_BROKER_URL)
        logging.info("KAFKA_TOPIC     : " + KAFKA_TOPIC)
        logging.info("KAFKA_SETTINGS  : " + str(KAFKA_SETTINGS))

        CONSUMER = Consumer(KAFKA_SETTINGS)
        CONSUMER.subscribe([KAFKA_TOPIC])

        print(f"Consuming from Kafka [{KAFKA_BROKER_URL}] - topic [{KAFKA_TOPIC}]")

        # Connect to database and get a cursor
        MYSQL_CONFIG = config["mysql"]
        MYSQL_URL = MYSQL_CONFIG["host"] + ":" + str(MYSQL_CONFIG["port"])
        MYSQL_DB = MYSQL_CONFIG["db"]
        MYSQL_TABLE = MYSQL_CONFIG["table"]

        logging.info("MYSQL_URL: " + MYSQL_URL)
        logging.info("MYSQL_DB : " + MYSQL_DB)

        DB_CONNECTION = utils.connect(MYSQL_CONFIG)
        DB_CURSOR = DB_CONNECTION.cursor()

        print(f"Writing to MySQL host [{MYSQL_URL}] - table [{MYSQL_DB}.{MYSQL_TABLE}]")

        params = dict(
            config=config,
            consumer=CONSUMER,
            db_connection=DB_CONNECTION,
            db_cursor=DB_CURSOR,
            db_table=MYSQL_TABLE,
        )

        return params

    @staticmethod
    def write_db(msg_df, msg_string, conf):
        try:
            # Create column list for insert statement
            cols = "`,`".join([str(i) for i in msg_df.columns.tolist()])
            logging.debug(cols)

            # Insert (replace duplicates) into database.
            for i, row in msg_df.iterrows():
                # Prepare sql statement
                sql = (
                    "REPLACE INTO `"
                    + conf['db_table']
                    + "` (`"
                    + cols
                    + "`) VALUES ("
                    + "%s," * (len(row) - 1)
                    + "%s)"
                )
                logging.debug(sql)
                logging.debug(tuple(row))

                # Execute sql statement providing values
                conf['db_cursor'].execute(sql, tuple(row))
                # The connection is not autocommitted by default, so we must commit to save our changes
                conf['db_connection'].commit()

            # Log
            logging.info(f"Write SUCCESS: [{msg_string}]")

        except:
            logging.warning(f"Write FAILURE [{msg_string}]")

    @staticmethod
    def process(msg_string, conf):
        try:
            # Load JSON string to dictionary object
            msg_dict = json.loads(msg_string)
            logging.debug(msg_dict)

            # Load dictionary object to dataframe
            msg_df = pd.DataFrame.from_dict([msg_dict])

            # Split created_at into two new columns
            calc = msg_df.apply(lambda row: pd.to_datetime(row.created_at), axis=1)

            # New column - created_dt: The datetime part up to microseconds (datetime)
            msg_df["created_dt"] = calc.apply(
                lambda x: x.replace(nanosecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")
            )

            # New column - created_ns: The nanoseconds part (integer)
            msg_df["created_ns"] = calc.dt.nanosecond.values.astype("int64")

            # Log debug
            logging.info(f"Process SUCCESS: [{msg_string}]")
            logging.debug(msg_df)
            logging.debug(msg_df.dtypes)

            # Write to database
            KafkaMySql.write_db(msg_df, msg_string, conf)

            # Return success
            return "SUCCESS"

        except:
            logging.warning(f"Process FAILURE: [{msg_string}]")

            # Return failure
            return "FAILURE"

    @staticmethod
    def run(env="dev"):
        conf = KafkaMySql.init(env)

        try:
            while True:
                msg = conf['consumer'].poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    msg_data = msg.value().decode("utf-8")
                    logging.info(f"Message: [{msg_data}]")
                    if msg_data == "Quit!":
                        break
                    print(KafkaMySql.process(msg_data, conf), " - ", msg_data)
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
            conf['consumer'].close()
