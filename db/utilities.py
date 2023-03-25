import os
import psycopg2

conn = psycopg2.connect(dbname="postgres", user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD'],
                        host=os.environ['DB_HOST'], port=os.environ['DB_PORT'])
cursor = conn.cursor()