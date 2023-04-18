import csv
import time
import argparse
from datetime import datetime
from typing import Tuple, List

from utilities import get_logger
from utilities import get_pg_connection

OUTPUT_FILEPATH_TEMPLATE = 'data/{source_type}/{specialization}/{source_upload_dt}'


def parse_args():
    """Getting  data from the user from the command line for loader PG"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--specialization', required=True, choices=['data_engineer'])
    parser.add_argument('--source_type', required=True, choices=['hh'])
    parser.add_argument('--source_upload_dt', required=True)
    parser.add_argument('--target_table', required=True, choices=['vacancies'])
    args = parser.parse_args()
    return vars(args)


class PostgresLoader():
    """Class for writing data to PostgreSQL"""

    def __init__(self, conn, logger) -> None:
        self.conn = conn if conn else get_pg_connection()
        self.cursor = self.conn.cursor()
        self.logger = logger if logger else get_logger()

    def create_temp_table(self, target_table: str, data: List[Tuple]):
        """Creating and insert data into a temporary table
        :param target_table: target table name
        :param data: data to write to the table
        :return: temporary table name
        """

        temp_table = f'{target_table}_{int(time.time())}'
        self.cursor.execute(f"SELECT * FROM {target_table} LIMIT 0")  # select columns name from target_table
        col_names = [col.name for col in self.cursor.description if col.name != 'id']  # identified columns without id

        query_create = f"""CREATE TEMP TABLE {temp_table} AS TABLE {target_table} WITH NO DATA"""
        self.logger.info(f'Create temporary table {temp_table}')
        try:
            self.cursor.execute(query_create)
            self.conn.commit()
            self.logger.info(f'Temporary table {temp_table} completed successfully')
        except:
            self.logger.error('Error creating temporary table')
            self.conn.rollback()

        self.logger.info(f'Inserting values into a temporary table {temp_table}')
        try:
            guery_insert_temp_table = f"""
                INSERT INTO {temp_table} 
                    ({', '.join(col_names)})
                VALUES ({', '.join(['%s'] * len(col_names))});
            """
            self.cursor.executemany(guery_insert_temp_table, data)
            self.conn.commit()
            self.logger.info(f'Inserting values into a temporary table {temp_table} completed successfully')
        except:
            self.logger.error('Error. No data entered into temporary table')
            self.conn.rollback()

        return temp_table, col_names

    def insert_and_update(self, target_table: str, data: List[Tuple]) -> None:
        """Update and insert value into target data table from temporary table
        :param target_table: target table name
        :param data: data to write to the table
        """

        temp_table, col_names = self.create_temp_table(target_table, data)
        self.logger.info(f'Update and insert value into target table {target_table} from temporary table {temp_table}')
        guery_insert_target_table = f"""
            INSERT INTO {target_table}
                ({', '.join(col_names)})
            SELECT {', '.join(col_names)}
            FROM {temp_table}
            ON CONFLICT
            DO NOTHING;
        """
        try:
            self.cursor.execute(guery_insert_target_table)
            self.conn.commit()
            self.logger.info(
                f'Update and insert table {target_table} data from temporary table {temp_table} completed successfully')
        except:
            self.logger.info('Error update and insert')
            self.conn.rollback()

    def insert_from_temp_into_target(self, target_table: str, data: List[Tuple], delete_condition=None) -> None:
        """Deleting data and adding new data
        :param target_table: target table name
        :param data: data to write to the table
        :param delete_condition: condition for deleting data
        """
        temp_table, col_names = self.create_temp_table(target_table, data)
        queries = []
        if delete_condition:
            q_delete = f"""DELETE FROM {target_table} {delete_condition};"""
            queries.append(q_delete)

        q_insert = f"""
            INSERT INTO {target_table} (
                {', '.join(col_names)}
            )
            SELECT {', '.join(col_names)}
            FROM {temp_table};
        """
        queries.append(q_insert)

        try:
            for query in queries:
                self.cursor.execute(query)
            self.conn.commit()
            self.logger.info('Data entered in the target table')
        except:
            self.logger.error('Query Execution Error')
            self.conn.rollback()

    def __del__(self):
        self.logger.info('Closing the connection')
        self.conn.close()


if __name__ == '__main__':

    args = parse_args()

    filepath = OUTPUT_FILEPATH_TEMPLATE.format(
        source_type=args['source_type'],
        specialization=args['specialization'],
        source_upload_dt=args['source_upload_dt'])
    output_filepath = f'{filepath}/vacancies.csv'

    update_ts = datetime.now()
    with open(output_filepath) as f:
        file_reader = csv.reader(f, delimiter=",")
        data = list(file_reader)
        data_from_csv = []  # creating list of data tuples from csv
        for row in data:
            line = row[0], update_ts, args['source_upload_dt'], args['source_type'], row[1], row[2], row[3], row[4], \
                row[5], row[6], \
                row[7], args['specialization']
            data_from_csv.append(line)

    delete_condition = f"""
        WHERE  specialization='{args['specialization']}'
        AND source_type='{args['source_type']}' 
        AND source_upload_dt='{args['source_upload_dt']}'
    """

    pg_loader = PostgresLoader(conn=get_pg_connection(), logger=get_logger())
    pg_loader.insert_from_temp_into_target(target_table=args['target_table'],
                                           data=data_from_csv,
                                           delete_condition=delete_condition)
    del pg_loader
