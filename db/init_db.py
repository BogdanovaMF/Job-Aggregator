import os
import psycopg2

conn = psycopg2.connect(dbname="postgres", user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD'], host="127.0.0.1", port=5433)
cursor = conn.cursor()

query_create_table = """CREATE TABLE IF NOT EXISTS vacancies
                        (id SERIAL PRIMARY KEY NOT NULL,
                        pub_date DATE,
                        vacancy VARCHAR,
                        experience VARCHAR,
                        company VARCHAR,
                        link VARCHAR,
                        salary VARCHAR,
                        skills VARCHAR,
                        description VARCHAR);"""

cursor.execute(query_create_table)
conn.commit()