import csv
import time
from pathlib import Path
from datetime import datetime
from typing import Tuple, List
from db.utilities import get_pg_connection


class PostgresLoader:
    """Class for writing data to PostgreSQL"""

    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    def create_temp_table(self, target_table: str, col_names: List, data: List[Tuple]):
        """Creating and insert data into a temporary table
        :param target_table: target table name
        :param col_names: list of table columns
        :param data: data to write to the table
        :return: temporary table name
        """

        current_tt = int(time.time())
        query_create = f"""
            CREATE TEMP TABLE {target_table}_{current_tt} 
            AS SELECT {', '.join(col_names)} 
            FROM {target_table} LIMIT 0
            ;
        """
        self.cursor.execute(query_create)

        item_placeholders = ', '.join(['%s'] * len(col_names))
        guery_insert_temp_table = f"""
            INSERT INTO {target_table}_{current_tt} 
            ({', '.join(col_names)})
            VALUES ({item_placeholders})
            ;
        """
        self.cursor.executemany(guery_insert_temp_table, data)
        self.conn.commit()

        return f'{target_table}_{current_tt}'

    def insert_and_update(self, target_table: str, temp_table: str, col_names: List):
        """Update and insert value into target data table from temporary table
        :param target_table: target table name
        :param temp_table: temporary table name
        :param col_names: list of table columns
        """

        guery_insert_target_table = f"""
            INSERT INTO {target_table}
            ({', '.join(col_names)})
            SELECT {', '.join(col_names)}
            FROM {temp_table}
            ON CONFLICT
            DO NOTHING
            ;
        """
        self.cursor.execute(guery_insert_target_table)
        self.conn.commit()

    def insert_from_temp_into_target(self, target_table: str, temp_table: str, col_names: List, delete_condition=None):
        """Deleting data and adding new data
        :param target_table: target table name
        :param temp_table: temporary table name
        :param col_names: list of table columns
        :param delete_condition: condition for deleting data
        """

        q1 = f"""
            DELETE FROM {target_table}
            {delete_condition}
            ;   
        """
        q2 = f"""
            INSERT INTO {target_table} (
                {', '.join(col_names)}
            )
            SELECT {', '.join(col_names)}
            FROM {temp_table}
            ;
        """

        try:
            if delete_condition is not None:
                self.cursor.execute(q1)
                self.cursor.execute(q2)
                self.conn.commit()
            else:
                self.cursor.execute(q2)
                self.conn.commit()
        except:
            self.conn.rollback()
            self.conn.commit()
        finally:
            self.conn.close()


if __name__ == '__main__':

    target_table = 'vacancies'
    source_upload_dt = datetime.now().date()
    source_type = 'hh'
    update_ts = datetime.now()
    specialization = 'data-engineer'

    delete_condition = f"""
        WHERE  specialization='{specialization}'
        AND source_type='{source_type}' 
        AND source_upload_dt='{source_upload_dt}'
    """

    OUTPUT_FILEPATH_TEMPLATE = 'data/{source_type}/{specialization}/{source_upload_dt}'.format(
        source_type=source_type,
        specialization=specialization,
        source_upload_dt=source_upload_dt)
    file = f'etl/{OUTPUT_FILEPATH_TEMPLATE}/vacancies.csv'
    pathfile = str(Path(Path.cwd(), file))

    with open(pathfile) as f:
        file_reader = csv.reader(f, delimiter=",")
        data = list(file_reader)
        data_from_csv = []  # creating list of data tuples from csv
        for row in data:
            line = row[0], update_ts, source_upload_dt, source_type, row[1], row[2], row[3], row[4], row[5], row[6], \
                row[7], specialization
            data_from_csv.append(line)

    conn = get_pg_connection()
    curr = conn.cursor()

    curr.execute(f"SELECT * FROM {target_table} LIMIT 0")  # select columns name from target_table
    col_names = [col.name for col in curr.description][1:]  # identified columns without id

    pg_loader = PostgresLoader(conn=conn)
    temp_table = pg_loader.create_temp_table(target_table, col_names, data_from_csv)

    pg_loader.insert_from_temp_into_target(target_table, temp_table, col_names, delete_condition)