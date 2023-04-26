import os
import csv
import sys
import logging
import psycopg2
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv

load_dotenv()


def get_pg_connection():
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
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


def save_data(data: List[Tuple], output_filepath: str):
    """We form the received information into the scv file."
    :param data: list with information on vacancies
    :param output_filepath: path to save the file
    """

    if not os.path.exists(output_filepath):
        os.makedirs(output_filepath)

    file = str(Path(output_filepath, 'vacancies.csv'))

    with open(file, 'wt') as f:
        csv_out = csv.writer(f)
        csv_out.writerows(data)
