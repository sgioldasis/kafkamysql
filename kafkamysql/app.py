import os
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd
from . import utils
import time
import numpy as np

# Main application class
class KafkaMySql:
    @staticmethod
    def init(env):
        """ Initialize connections to Kafka and database """
        # Load configuration
        config = utils.load_config(env)

        # Connect to Kafka and subscribe to topic
        kafka_broker_url = config["kafka"]["broker_url"]
        kafka_topic = config["kafka"]["topic"]
        kafka_settings = config["kafka"]["settings"]

        logging.info("Kafka broker_url: " + kafka_broker_url)
        logging.info("Kafka topic     : " + kafka_topic)
        logging.info("Kafka settings  : " + str(kafka_settings))

        kafka_consumer = Consumer(kafka_settings)
        kafka_consumer.subscribe([kafka_topic])

        print(f"Consuming from Kafka [{kafka_broker_url}] - topic [{kafka_topic}]")

        # Connect to database and get a cursor
        mysql_config = config["mysql"]
        mysql_url = mysql_config["host"] + ":" + str(mysql_config["port"])
        mysql_db = mysql_config["db"]
        mysql_table = mysql_config["table"]

        logging.info("mysql_url: " + mysql_url)
        logging.info("mysql_db : " + mysql_db)

        mysql_connection = utils.connect(mysql_config)
        mysql_cursor = mysql_connection.cursor()

        print(f"Writing to MySQL host [{mysql_url}] - table [{mysql_db}.{mysql_table}]")

        # Return dictionary
        return dict(
            kafka_consumer=kafka_consumer,
            db_connection=mysql_connection,
            db_cursor=mysql_cursor,
            db_table=mysql_table,
            max_records=config["batch"]["max_records"],
            max_seconds=config["batch"]["max_seconds"],
        )

    @staticmethod
    def write_db(msg_df, msg_list, conf):
        """ Write a dataframe to the database """
        try:

            # Replace null values with None
            msg_df = msg_df.astype(object).where(pd.notnull(msg_df), None)

            # Get table, columns and data
            table = conf["db_table"]
            cols = msg_df.columns.tolist()
            data = msg_df.to_dict("records")

            # SQL Insert statement
            sql = (
                "INSERT IGNORE INTO "
                + table
                + " ("
                + ",".join(cols)
                + ") values "
                + ",".join(["(" + ",".join(["%s"] * len(cols)) + ")"] * len(data))
            )

            # Values for SQL statement
            data_values = tuple([row[col] for row in data for col in cols])

            # Execute sql statement providing values
            cursor = conf["db_connection"].cursor()
            cursor.execute(sql, data_values)

            # Warnings
            warnings = cursor.fetchwarnings()
            if warnings is not None:
                with open("logs/warnings.txt", "a") as warnings_file:
                    for warning in warnings:
                        warnings_file.write(str(warning) + "\n")

            # Close cursor
            cursor.close()

            # The connection is not autocommitted by default, so we must commit to save
            conf["db_connection"].commit()

            # Log
            logging.debug(f"Write SUCCESS: [{msg_list}]")

        except Exception as e:
            logging.warning(f"Write FAILURE {msg_list}" + str(e))

    @staticmethod
    def process(msg_list, conf):
        """ Convert a list of messages to dataframe and calculate new columns """
        try:

            # Load dictionary object to dataframe
            msg_df = pd.DataFrame.from_dict(msg_list, orient="columns")

            # Split created_at into two new columns
            calc = msg_df.apply(lambda row: pd.to_datetime(row.created_at), axis=1)

            # New column - created_dt: The datetime part up to microseconds (datetime)
            msg_df["created_dt"] = calc.apply(
                lambda x: x.replace(nanosecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")
                if pd.notnull(x)
                else x
            )

            # New column - created_ns: The nanoseconds part (integer)
            msg_df["created_ns"] = calc.dt.nanosecond.values.astype("int64")

            # Log debug
            logging.debug(f"Process SUCCESS: [{msg_list}]")

            # Write to database
            KafkaMySql.write_db(msg_df, msg_list, conf)

            # Return success
            return "SUCCESS"

        except Exception as e:
            logging.warning(f"Process FAILURE {e} : \n{msg_list}")

            # Return failure
            return "FAILURE"

    @staticmethod
    def buffer(msg_data, msg_list):
        """ Buffer individual messages into list """
        try:
            # Load JSON string to dictionary object
            msg_dict = json.loads(msg_data)

            # Validate id column
            if msg_dict["id"] is None or len(msg_dict["id"]) == 0:
                raise ValueError("Column 'id' cannot be null")

            # Validate created_at column
            pd.Timestamp(msg_dict["created_at"])

            # Append dictionary object to the list
            msg_list.append(msg_dict)

            return 0

        except Exception as e:
            logging.warning(
                f"Buffer: Validation FAILURE: {e} \nOffending data: \n{msg_data}"
            )
            with open("logs/rejected.txt", "a") as rejected_file:
                rejected_file.write(msg_data + " --> " + str(e) + "\n")

            return 1

    @staticmethod
    def run(env="dev"):
        """ Main consumer function """

        # Initialization
        conf = KafkaMySql.init(env)
        consumer = conf["kafka_consumer"]
        msg_num = 0
        start = time.time()
        msg_count = 0
        rejected_count = 0
        msg_list = []
        msg_last = None
        msg_data = ""

        # Main consumer loop
        try:

            while True:
                msg = consumer.poll(0.1)
                elapsed = time.time() - start

                # Handle message batch
                if (
                    elapsed >= conf["max_seconds"]
                    or len(msg_list) == conf["max_records"]
                ):
                    if len(msg_list) > 0:
                        print(
                            KafkaMySql.process(msg_list, conf),
                            ": ",
                            msg_num,
                            " - Rejected: ",
                            rejected_count,
                        )
                        consumer.commit(message=msg_last, async=False)
                        msg_list.clear()
                        msg_last = None
                        start = time.time()

                    if msg_data == "Quit!":
                        break

                # Handle individual messages
                if msg is None:
                    continue

                elif not msg.error():
                    msg_data = msg.value().decode("utf-8")
                    msg_num += 1
                    logging.debug(f"Message: [{msg_data}]")

                    rejected_count += KafkaMySql.buffer(msg_data, msg_list)
                    msg_last = msg

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
            conf["kafka_consumer"].close()
