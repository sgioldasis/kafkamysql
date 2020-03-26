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

    # @staticmethod
    # def write_db(msg_df, msg_string, conf):
    #     try:
    #         msg_df.to_csv('buffered.csv', encoding='utf-8', header = True, doublequote = True, sep=',', index=False)

    @staticmethod
    def write_db(msg_df, msg_list, conf):
        try:

            msg_df = msg_df.astype(object).where(pd.notnull(msg_df),None)
            table = conf["db_table"]

            # Create column list for insert statement
            # cols = "`,`".join([str(i) for i in msg_df.columns.tolist()])
            # logging.debug(cols)

            # Prepare sql statement: Insert (replace duplicates)
            # sql = (
            #     "REPLACE INTO `"
            #     + conf["db_table"]
            #     + "` (`"
            #     + cols
            #     + "`) values ("
            #     + "%s," * (len(msg_df.columns.tolist()) - 1)
            #     + "%s)"
            # )
            # logging.info(sql)

            # data = []
            # data = [tuple(None if type(y) == float and np.isnan(y) else y for y in x) for x in msg_df.values]
            # data = [tuple(None if pd.isnull(y) else y for y in x) for x in msg_df.values]

            # for i, row in msg_df.iterrows():
            #     logging.info(tuple(row))
            #     data.append(tuple(row))

            cols = msg_df.columns.tolist()
            logging.info('---------- cols')
            logging.info(cols)

            # data = msg_df.values.tolist()
            data = msg_df.to_dict('records')
            logging.info('---------- data')
            logging.info(data)

            sql = "INSERT IGNORE INTO " + table + " (" + ",".join(cols) + ") values " + ",".join(["(" + ",".join(["%s"] * len(cols)) + ")"] * len(data))
            logging.info('---------- sql')
            logging.info(sql)

            data_values = tuple([row[col] for row in data for col in cols])
            logging.info('---------- data_values')
            logging.info(data_values)

            # Execute sql statement providing values
            cursor = conf["db_connection"].cursor()
            cursor.execute(sql, data_values)


            logging.info('---------- warnings')
            warnings = cursor.fetchwarnings()
            if warnings is not None:
                with open("warnings.txt", "a") as warnings_file:
                    for warning in warnings:
                        warnings_file.write(str(warning) + "\n")


            cursor.close()

            # The connection is not autocommitted by default, so we must commit to save
            conf["db_connection"].commit()

            # Log
            logging.info(f"Write SUCCESS: [{msg_list}]")

        except Exception as e:
            logging.warning(f"Write FAILURE {msg_list}" + str(e))

    @staticmethod
    def process(msg_list, conf):
        try:

            # Load dictionary object to dataframe
            # msg_df = pd.DataFrame.from_dict(msg_list, orient='columns')
            logging.warning(f"Processing: {msg_list}")
            msg_df = pd.DataFrame.from_dict(msg_list, orient="columns")
            logging.warning(msg_df)

            # Split created_at into two new columns
            calc = msg_df.apply(lambda row: pd.to_datetime(row.created_at), axis=1)

            # New column - created_dt: The datetime part up to microseconds (datetime)
            msg_df["created_dt"] = calc.apply(
                lambda x: x.replace(nanosecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")
            )

            # New column - created_ns: The nanoseconds part (integer)
            msg_df["created_ns"] = calc.dt.nanosecond.values.astype("int64")

            # Log debug
            logging.info(f"Process SUCCESS: [{msg_list}]")
            logging.debug(msg_df)
            logging.debug(msg_df.dtypes)

            # Write to database
            KafkaMySql.write_db(msg_df, msg_list, conf)

            # Return success
            return "SUCCESS"

        except Exception as e:
            logging.warning(f"Process FAILURE: [{msg_list}]")
            # raise e

            # Return failure
            return "FAILURE"

    @staticmethod
    def buffer(msg_data, msg_list):
        try:
            # Load JSON string to dictionary object
            msg_dict = json.loads(msg_data)
            logging.debug(msg_dict)

            # Append dictionary object to the list
            msg_list.append(msg_dict)

        except Exception as e:
            logging.warning(f"Buffer FAILURE: [{msg_data}]")
            print(f"Buffer FAILURE: [{msg_data}]")
            with open("rejected.txt", "a") as rejected_file:
                rejected_file.write(msg_data + " --> " + str(e) + "\n")

        finally:
            return msg_list

    @staticmethod
    def run(env="dev"):
        conf = KafkaMySql.init(env)
        consumer = conf["kafka_consumer"]
        msg_num = 0
        start = time.time()
        msg_count = 0
        msg_list = []
        msg_last = None
        msg_data = ""

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
                        logging.warning(
                            "*** Elapsed="
                            + str(elapsed)
                            + ", len(msg_list)="
                            + str(len(msg_list))
                        )
                        print(KafkaMySql.process(msg_list, conf), " - ", msg_num)
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
                    logging.info(f"Message: [{msg_data}]")

                    KafkaMySql.buffer(msg_data, msg_list)
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
