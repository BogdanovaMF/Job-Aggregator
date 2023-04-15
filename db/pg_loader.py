import csv
import time
from datetime import datetime
from typing import Tuple, List
from abc import abstractmethod, ABC

from utilities import parse_args
from utilities import get_logger
from utilities import get_pg_connection

OUTPUT_FILEPATH_TEMPLATE = 'data/{source_type}/{specialization}/{source_upload_dt}'


class Loader(ABC):
    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    @abstractmethod
    def columns_names(self, target_table):
        self.cursor.execute(f"SELECT * FROM {target_table} LIMIT 0")  # select columns name from target_table


class PostgresLoader(Loader):
    """Class for writing data to PostgreSQL"""

    def __init__(self, conn):
        super().__init__(conn)

    def columns_names(self, target_table):
        super().columns_names(target_table)
        col_names = [col.name for col in self.cursor.description if col.name != 'id']  # identified columns without id
        logger.info('Getting a list of columns')
        return col_names

    def create_temp_table(self, target_table: str, data: List[Tuple]):
        """Creating and insert data into a temporary table
        :param target_table: target table name
        :param data: data to write to the table
        :return: temporary table name
        """
        col_names = self.columns_names(target_table)
        current_tt = int(time.time())
        logger.info(f'Create temporary table {target_table}_{current_tt}')
        query_create = f"""
            CREATE TEMP TABLE {target_table}_{current_tt} 
            AS SELECT {', '.join(col_names)} 
            FROM {target_table} 
            LIMIT 0;
        """
        try:
            self.cursor.execute(query_create)
            logger.info(f'Temporary table {target_table}_{current_tt} completed successfully')
        except:
            logger.error('Error creating temporary table')

        item_placeholders = ', '.join(['%s'] * len(col_names))

        logger.info(f'Inserting values into a temporary table {target_table}_{current_tt}')
        try:
            guery_insert_temp_table = f"""
                INSERT INTO {target_table}_{current_tt} 
                    ({', '.join(col_names)})
                VALUES ({item_placeholders});
            """
            self.cursor.executemany(guery_insert_temp_table, data)
            self.conn.commit()
            logger.info(f'Inserting values into a temporary table {target_table}_{current_tt} completed successfully')
        except:
            logger.error('Error. No data entered into temporary table')

        return f'{target_table}_{current_tt}'

    def insert_and_update(self, target_table: str, temp_table: str):
        """Update and insert value into target data table from temporary table
        :param target_table: target table name
        :param temp_table: temporary table name
        """

        col_names = self.columns_names(target_table)
        logger.info(f'Update and insert value into target table {target_table} from temporary table {temp_table}')
        guery_insert_target_table = f"""
            INSERT INTO {target_table}
                ({', '.join(col_names)})
            SELECT {', '.join(col_names)}
            FROM {temp_table}
            ON CONFLICT
            DO NOTHING;
        """
        self.cursor.execute(guery_insert_target_table)
        self.conn.commit()
        logger.info(
            f'Update and insert table {target_table} data from temporary table {temp_table} completed successfully')

    def insert_from_temp_into_target(self, target_table: str, temp_table: str, delete_condition=None):
        """Deleting data and adding new data
        :param target_table: target table name
        :param temp_table: temporary table name
        :param delete_condition: condition for deleting data
        """

        col_names = self.columns_names(target_table)
        q1 = f"""
            DELETE FROM {target_table}
            {delete_condition};   
        """
        q2 = f"""
            INSERT INTO {target_table} (
                {', '.join(col_names)}
            )
            SELECT {', '.join(col_names)}
            FROM {temp_table};
        """
        queries = q1 + q2
        try:
            if delete_condition is not None:
                self.cursor.execute(queries)
                self.conn.commit()
                logger.info('Both requests completed. Removed old data and added new ones')
            else:
                self.cursor.execute(q2)
                self.conn.commit()
                logger.info('Only one insert request completed')
        except:
            logger.error('Query Execution Error')
            self.conn.rollback()

    def __del__(self):
        logger.info('Closing the connection')
        self.conn.close()


if __name__ == '__main__':

    args = parse_args()
    logger = get_logger()
    conn = get_pg_connection()

    delete_condition = f"""
        WHERE  specialization='{args['specialization']}'
        AND source_type='{args['source_type']}' 
        AND source_upload_dt='{args['source_upload_dt']}'
    """

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

    pg_loader = PostgresLoader(conn=conn)
    temp_table = pg_loader.create_temp_table(args['target_table'], data_from_csv)
    pg_loader.insert_from_temp_into_target(args['target_table'], temp_table, delete_condition)
    del pg_loader
