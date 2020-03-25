import os
import mysql.connector
import yaml

RESOURCE_FOLDER = "resources"


def load_config(env='dev'):
    config_filename = f'config.{env}.yml'

    dirname = os.path.dirname(__file__)
    config_file = os.path.join(dirname, config_filename)
    # Load config
    with open(config_file, "r") as ymlfile:
        config = yaml.safe_load(ymlfile)

    return config


def connect(config):
    # Connect to database
    db_connection = mysql.connector.connect(
        host=config["host"],
        port=config["port"],
        database=config["db"],
        user=config["user"],
        passwd=config["passwd"],
    )

    return db_connection


def get_sql(file_name):
    with open(f"{RESOURCE_FOLDER}/{file_name}", "r") as f:
        return f.read()
