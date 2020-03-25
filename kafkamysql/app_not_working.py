import os
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd
from . import utils

BUFFER = []

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
        mysql_commit_every = mysql_config["commit_every"]

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
            db_commit_every=mysql_commit_every,
        )

    @staticmethod
    def commit_db(sql, conf):
        try:
            # logging.info("**************** Writing to db")
            # logging.info(BUFFER)
            print(BUFFER)
            sys.exit(0)

            # Execute sql statement providing values
            conf["db_cursor"].executemany(sql, BUFFER)

            # The connection is not autocommitted by default, so we must commit to save our changes
            conf["db_connection"].commit()

            # Log
            logging.info(f"Write SUCCESS")

            BUFFER.clear()

        except:
            logging.warning(f"Write FAILURE")


    @staticmethod
    def write_db(msg_df, msg_string, msg_num, conf):
        # Create column list for insert statement
        cols = "`,`".join([str(i) for i in msg_df.columns.tolist()])
        logging.debug(cols)

        # Prepare sql statement
        sql = (
            "REPLACE INTO `"
            + conf["db_table"]
            + "` (`"
            + cols
            + "`) VALUES ("
            + "%s," * (msg_df.shape[1] - 1)
            + "%s)"
        )
        logging.debug(sql)

        # Insert (replace duplicates) into database.
        for i, row in msg_df.iterrows():
            BUFFER.append(tuple(row))
            logging.debug(tuple(row))

        if msg_num % 10 == 0:
            logging.info("**************** Writing to db, msg_num "+str(msg_num))
            print("**************** Writing to db, msg_num "+str(msg_num))
            logging.info(BUFFER)
            KafkaMySql.commit_db(sql, conf)
            sys.exit(0)

        # Log
        logging.info(f"Write SUCCESS: [{msg_string}]")

    @staticmethod
    def process(msg_string, msg_num, conf):
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
            # logging.info('**************** msg_num = '+str(msg_num))
            # logging.warning(conf['db_commit_every'])
            KafkaMySql.write_db(msg_df, msg_string, msg_num, conf)

            # Return success
            return "SUCCESS"

        except:
            logging.warning(f"Process FAILURE: [{msg_string}]")

            # Return failure
            return "FAILURE"

    @staticmethod
    def run(env="dev"):
        conf = KafkaMySql.init(env)
        msg_num = 0

        try:
            while True:
                msg = conf["kafka_consumer"].poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    msg_data = msg.value().decode("utf-8")
                    msg_num += 1
                    logging.info(f"Message: [{msg_data}]")
                    if msg_data == "Quit!":
                        break
                    print(str(msg_num), " : ", KafkaMySql.process(msg_data, msg_num, conf), " - ", msg_data)
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