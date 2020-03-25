import os
import mysql.connector
import yaml

RESOURCE_FOLDER = 'resources'

def load_config():
    dirname = os.path.dirname(__file__)
    config_file = os.path.join(dirname, "config.yml")
    # Load config
    with open(config_file, "r") as ymlfile:
        config = yaml.safe_load(ymlfile)

    return config


def connect():
    # Load config
    config = load_config()

    # Connect to database
    db_connection = mysql.connector.connect(
        host=config["mysql"]["host"],
        port=config["mysql"]["port"],
        database=config["mysql"]["db"],
        user=config["mysql"]["user"],
        passwd=config["mysql"]["passwd"],
    )

    return db_connection


def get_sql(file_name):
    with open(f"{RESOURCE_FOLDER}/{file_name}", "r") as f:
        return f.read()
