import os
import sys
import logging
import argparse
import psycopg2


def get_pg_connection():
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )
    return conn


def get_logger():
    """Object logging function"""

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s  %(asctime)s: %(message)s"))
    logger.addHandler(handler)

    fh = logging.FileHandler('./file.log')
    fh.setFormatter(logging.Formatter("%(levelname)s  %(asctime)s: %(message)s"))
    logger.addHandler(fh)
    return logger


def parse_args():
    """Getting  data from the user from the command line for loader PG"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--specialization', required=True, choices=['data_engineer'])
    parser.add_argument('--source_type', required=True, choices=['hh'])
    parser.add_argument('--source_upload_dt', required=True)
    parser.add_argument('--target_table', required=True, choices=['vacancies'])
    args = parser.parse_args()
    return vars(args)
