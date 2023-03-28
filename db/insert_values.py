import csv
from os.path import getctime
from datetime import datetime
from db.utilities import get_pg_connection


def get_values(data_dict):
    """function for forming a tuple for passing to a sql table
    :param data_dict: DictReader object - dictionary with string values from csv-file
    :return: result tuple
    """

    # file creation date
    source_upload_dt = datetime.fromtimestamp(getctime(filename)).strftime('%Y-%m-%d')
    source_upload_dt = datetime.strptime(source_upload_dt, '%Y-%m-%d').date()

    # Source of information received about vacancies
    source_type = 'hh'

    # converting the date to the desired format
    try:
        pub_date = datetime.strptime(data_dict['pub_date'], '%Y-%m-%d').date()
    except:
        pub_date = None

    # date the table was modified
    update_ts = datetime.now()

    result = pub_date, update_ts, source_upload_dt, source_type, data_dict['vacancy'], data_dict['experience'], \
        data_dict['company'], data_dict['link'], data_dict['salary'], data_dict['skills'], data_dict['description']
    return result


if __name__ == '__main__':

    filename = '/Users/marinabogdanova/workspace/Job-Aggregator/result/data-engineer_2023-03-19 16:49:36.227171.csv'
    conn = get_pg_connection()
    cursor = conn.cursor()

    # query to enter data into a table
    guery = """
        INSERT INTO vacancies(
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
            description
    )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    # create a DictReader object, specify the separator character ","
    with open(filename, encoding='utf-8') as f:
        file_reader = csv.DictReader(f, delimiter=",")

        # reading data from a CSV file
        for row in file_reader:
            data = get_values(row)
            cursor.execute(guery, data)
            conn.commit()