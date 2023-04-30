import csv
import time
import argparse
from datetime import datetime
from typing import Tuple, List, Optional

from utils import get_logger
from utils import get_pg_connection
from config import OUTPUT_FILEPATH_TEMPLATE


def parse_args():
    """Getting  data from the user from the command line for loader PG"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--specialization', required=True, choices=['data_engineer'])
    parser.add_argument('--source_type', required=True, choices=['hh'])
    parser.add_argument('--source_upload_dt', required=True)
    parser.add_argument('--target_table', required=True, choices=['vacancies'])
    args = parser.parse_args()
    return vars(args)


class PostgresLoader:
    """Class for writing data to PostgreSQL"""

    def __init__(self, conn, logger) -> None:
        self.conn = conn if conn else get_pg_connection()
        self.cursor = self.conn.cursor()
        self.logger = logger if logger else get_logger()

    def insert_data_into_table(self, table_name: str, data: List[Tuple],
                               create_like_table: Optional[str] = None) -> None:
        """Create temp table. Insert data into a table.
        :param table_name: tables name for insert values
        :param data: data to write to the table
        :param create_like_table: the name of the table whose structure is to be copied
        """

        query_create = f"""CREATE TEMP TABLE {table_name} AS TABLE {create_like_table} WITH NO DATA"""
        try:
            self.cursor.execute(query_create)
            self.conn.commit()
            self.logger.info(f'Temporary table {table_name} completed successfully')
        except Exception as ex:
            self.logger.error(f'Some error occurred: {ex}')
            self.conn.rollback()
            raise ex

        self.cursor.execute(f"SELECT * FROM {create_like_table} LIMIT 0")  # select columns name from table
        col_names = [col.name for col in self.cursor.description if col.name != 'id']  # identified columns without id
        try:
            guery_insert_data = f"""
                INSERT INTO {table_name} 
                    ({', '.join(col_names)})
                VALUES ({', '.join(['%s'] * len(col_names))});
            """
            self.cursor.executemany(guery_insert_data, data)
            self.conn.commit()
            self.logger.info(f'Inserting values into a table {table_name} completed successfully')
        except Exception as ex:
            self.logger.error(f'Some error occurred: {ex}')
            self.conn.rollback()
            raise ex

    def insert_from_temp_into_target(self, target_table: str, temp_table: str,
                                     delete_condition: Optional[str] = None) -> None:
        """Deleting old data and adding new data
        :param target_table: target table name
        :param temp_table: temporary table name
        :param delete_condition: condition for deleting data
        """

        self.cursor.execute(f"SELECT * FROM {target_table} LIMIT 0")  # select columns name from target_table
        col_names = [col.name for col in self.cursor.description if col.name != 'id']  # identified columns without id

        queries = []
        if delete_condition:
            q_delete = f"""DELETE FROM {target_table} {delete_condition};"""
            queries.append(q_delete)
        q_insert = f"""INSERT INTO {target_table} ({', '.join(col_names)}) SELECT {', '.join(col_names)} FROM {temp_table};"""
        queries.append(q_insert)
        queries.append(f"""DROP TABLE {temp_table};""")

        try:
            for query in queries:
                self.cursor.execute(query)
            self.logger.info(f'Inserting data from a temporary table {temp_table} into the targer table "{target_table}"')
            self.conn.commit()
        except Exception as ex:
            self.logger.error(f'Some error occurred: {ex}')
            self.conn.rollback()
            raise ex

    def __del__(self):
        self.logger.info('Closing the connection')
        self.conn.close()


if __name__ == '__main__':
    args = parse_args()
    target_table = args['target_table']
    temp_table = f'{target_table}_{int(time.time())}'

    filepath = OUTPUT_FILEPATH_TEMPLATE.format(
        source_type=args['source_type'],
        specialization=args['specialization'],
        source_upload_dt=args['source_upload_dt'])
    output_filepath = f'{filepath}/vacancies.csv'

    update_ts = datetime.now()
    with open(output_filepath) as f:
        data = list(csv.reader(f, delimiter=","))
        data_from_csv = [
            (row[0], update_ts, args['source_upload_dt'], args['source_type'], row[1], row[2], row[3], row[4],
             row[5], row[6], row[7], args['specialization']) for row in data]  # creating list of data tuples from csv

    delete_condition = f"""
        WHERE  specialization='{args['specialization']}'
        AND source_type='{args['source_type']}' 
        AND source_upload_dt='{args['source_upload_dt']}'
    """

    pg_loader = PostgresLoader(conn=get_pg_connection(), logger=get_logger())
    pg_loader.insert_data_into_table(table_name=temp_table,
                                     data=data_from_csv,
                                     create_like_table=target_table)
    pg_loader.insert_from_temp_into_target(target_table=target_table,
                                           temp_table=temp_table,
                                           delete_condition=delete_condition)
    del pg_loader
