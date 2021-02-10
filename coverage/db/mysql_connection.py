import logging
import mysql.connector
import yaml

from contextlib import contextmanager


def load_configuration(config_file):
    logging.info("Loading configuration from file {}".format(config_file))
    with open(config_file, "r") as file_stream:
        return yaml.safe_load(file_stream)


@contextmanager
def connect_to_database(config, autocommit=True):
    logging.debug("Connecting to MySQL database")
    with mysql.connector.connect(
        host=config["host"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
        autocommit=autocommit
    ) as dbcon:
        yield dbcon
