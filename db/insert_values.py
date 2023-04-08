import csv
import time
from pathlib import Path
from datetime import datetime
from typing import Tuple, List
from db.utilities import get_pg_connection


class PostgresLoader:
    """Class for writing data to PostgreSQL"""

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def _create_temp_table(self, target_table: str, data: List[Tuple], source_type: str, specialization: str):
        """Creating and insert data into a temporary table
        :param target_table: target table name
        :param data: data to write to the table
        :param source_type: the type of resource from which the data came
        :param specialization: specialization for which vacancies are searched
        :return: temporary table name
        """

        current_tt = int(time.time())
        query = f"""
            CREATE TEMP TABLE {target_table}_{current_tt} (
                pub_date DATE,
                update_ts TIMESTAMP WITH TIME ZONE,
                source_upload_dt DATE,
                source_type VARCHAR,
                vacancy VARCHAR,
                experience VARCHAR,
                company VARCHAR,
                link VARCHAR,
                salary VARCHAR,
                skills VARCHAR,
                description VARCHAR,
                specialization VARCHAR
            );
            """
        self.cursor.execute(query)

        guery_insert_temp_table = f"""
            INSERT INTO {target_table}_{current_tt}(
                pub_date,
                update_ts,
                source_upload_dt,
                source_type,
                vacancy,
                experience,
                company,
                link,
                salary,
                skills,
                description,
                specialization
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ;
            """

        # file creation date
        source_upload_dt = datetime.now().date()

        # date the table was modified
        update_ts = datetime.now()

        for i in data:
            line = i[0], update_ts, source_upload_dt, source_type, i[1], i[2], i[3], i[4], i[5], i[6], i[
                7], specialization
            self.cursor.execute(guery_insert_temp_table, line)

        return f'{target_table}_{current_tt}'

    def insert_and_update(self, target_table: str):
        """Update and insert value into target data table from temporary table
        :param target_table: target table name
        """

        temp_table = self._create_temp_table(target_table, data, source_type, specialization)

        guery_insert_target_table = f"""
            INSERT INTO {target_table}(
                pub_date,
                update_ts,
                source_upload_dt,
                source_type,
                vacancy,
                experience,
                company,
                link,
                salary,
                skills,
                description,
                specialization
                )
            SELECT
                pub_date,
                update_ts,
                source_upload_dt,
                source_type,
                vacancy,
                experience,
                company,
                link,
                salary,
                skills,
                description,
                specialization
            FROM {temp_table}
            ON CONFLICT
            DO NOTHING
            ;
        """
        self.cursor.execute(guery_insert_target_table)
        self.conn.commit()

    def insert_from_temp_into_target(self, target_table: str, delete_condition):
        """Deleting data and adding new data
        :param target_table: target table name
        :param delete_condition: condition for deleting data
        """

        temp_table = self._create_temp_table(target_table, data, source_type, specialization)
        q1 = f"""
            DELETE FROM {target_table} 
            WHERE specialization ='{delete_condition[0]}'
              AND source_type='{delete_condition[1]}' 
              AND source_upload_dt='{delete_condition[2]}'
            ;
        """
        q2 = f"""
            INSERT INTO {target_table}(
                pub_date,
                update_ts,
                source_upload_dt,
                source_type,
                vacancy,
                experience,
                company,
                link,
                salary,
                skills,
                description,
                specialization
                )
            SELECT
                pub_date,
                update_ts,
                source_upload_dt,
                source_type,
                vacancy,
                experience,
                company,
                link,
                salary,
                skills,
                description,
                specialization
            FROM {temp_table}
            ;
        """
        self.cursor.execute(q1)
        self.cursor.execute(q2)
        self.conn.commit()


if __name__ == '__main__':

    target_table = 'vacancies'

    # file creation date
    source_upload_dt = datetime.now().date()

    # Source of information received about vacancies
    source_type = 'hh'

    specialization = 'data-engineer'

    delete_condition = (specialization, source_type, source_upload_dt)

    file = 'etl/data/{source_type}/{specialization}/{source_upload_dt}/vacancies.csv'.format(
        source_type=source_type,
        specialization=specialization,
        source_upload_dt=source_upload_dt)

    pathfile = str(Path(Path.cwd(), file))

    with open(pathfile) as f:
        file_reader = csv.reader(f, delimiter=",")
        data = list(file_reader)

    conn = get_pg_connection()
    cursor = conn.cursor()
    pg_loader = PostgresLoader(conn=conn, cursor=cursor)

    pg_loader.insert_from_temp_into_target(target_table, delete_condition)