import os
from time import sleep
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd
from . import utils


# Load configuration
config = utils.load_config()

# Connect to Kafka and subscribe to topic
KAFKA_BROKER_URL = config["kafka"]["broker_url"]
KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_SETTINGS = config["kafka"]["settings"]

logging.info("KAFKA_BROKER_URL: " + KAFKA_BROKER_URL)
logging.info("KAFKA_TOPIC     : " + KAFKA_TOPIC)
logging.info("KAFKA_SETTINGS  : " + str(KAFKA_SETTINGS))

CONSUMER = Consumer(KAFKA_SETTINGS)
CONSUMER.subscribe([KAFKA_TOPIC])

# Connect to database and get a cursor
MYSQL_URL = config["mysql"]["host"] + ":" + str(config["mysql"]["port"])
MYSQL_DB = config["mysql"]["db"]
MYSQL_TABLE = config["mysql"]["table"]

logging.info("MYSQL_URL: " + MYSQL_URL)
logging.info("MYSQL_DB : " + MYSQL_DB)

DB_CONNECTION = utils.connect()
DB_CURSOR = DB_CONNECTION.cursor()


# Main application class
class KafkaMySql:
    @staticmethod
    def db_write(msg_df, msg_string):
        try:
            # Create column list for insert statement
            cols = "`,`".join([str(i) for i in msg_df.columns.tolist()])
            logging.debug(cols)

            # Insert (replace duplicates) into database.
            for i, row in msg_df.iterrows():
                # Prepare sql statement
                sql = (
                    "REPLACE INTO `"
                    + MYSQL_TABLE
                    + "` (`"
                    + cols
                    + "`) VALUES ("
                    + "%s," * (len(row) - 1)
                    + "%s)"
                )
                logging.debug(sql)
                logging.debug(tuple(row))

                # Execute sql statement providing values
                DB_CURSOR.execute(sql, tuple(row))
                # The connection is not autocommitted by default, so we must commit to save our changes
                DB_CONNECTION.commit()

            # Log
            logging.info(f"Write SUCCESS: [{msg_string}]")

        except:
            logging.warning(f"Write FAILURE [{msg_string}]")

    @staticmethod
    def process(msg_string):
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
            KafkaMySql.db_write(msg_df, msg_string)

            # Return success
            return "SUCCESS"

        except:
            logging.warning(f"Process FAILURE: [{msg_string}]")

            # Return failure
            return "FAILURE"

    @staticmethod
    def run():
        print(f"Consuming from Kafka [{KAFKA_BROKER_URL}] - topic [{KAFKA_TOPIC}]")
        print(f"Writing to MySQL host [{MYSQL_URL}] - table [{MYSQL_DB}.{MYSQL_TABLE}]")

        try:
            while True:
                msg = CONSUMER.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    msg_data = msg.value().decode("utf-8")
                    logging.info(f"Message: [{msg_data}]")
                    if msg_data == "Quit!":
                        break
                    print(KafkaMySql.process(msg_data), " - ", msg_data)
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
            CONSUMER.close()
