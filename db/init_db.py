from utilities import get_pg_connection

if __name__ == '__main__':
    query = """ 
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            pub_date DATE,
            update_ts TIMESTAMP WITH TIME ZONE,
            csv_date TIMESTAMP WITH TIME ZONE,
            source_type CHARACTER VARYING (20),
            vacancy CHARACTER VARYING (150),
            experience CHARACTER VARYING (50),
            company CHARACTER VARYING (128),
            link CHARACTER VARYING (100),
            salary CHARACTER VARYING (50),
            skills CHARACTER VARYING (300),
            description VARCHAR
        ); 
    """

    conn = get_pg_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()