from utilities import get_pg_connection

if __name__ == '__main__':
    query = """ 
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            pub_date DATE,
            update_ts TIMESTAMP WITH TIME ZONE,
            source_upload_dt TIMESTAMP WITH TIME ZONE,
            source_type CHARACTER VARYING (20),
            vacancy CHARACTER VARYING (150),
            experience CHARACTER VARYING (50),
            company CHARACTER VARYING (128),
            link CHARACTER VARYING (100),
            salary CHARACTER VARYING (50),
            skills CHARACTER VARYING (300),
            description VARCHAR
        );
        COMMENT ON COLUMN vacancies.pub_date 
            IS 'Дата публикации вакансии';
        COMMENT ON COLUMN vacancies.update_ts 
            IS 'Дата вставки данных в таблицу vacancies';
        COMMENT ON COLUMN vacancies.source_upload_dt 
            IS 'Дата создания csv-файла, из которого выгружается информация';
        COMMENT ON COLUMN vacancies.source_type 
            IS 'Источник полученной инфомарции о вакансиях ';
        COMMENT ON COLUMN vacancies.vacancy 
            IS 'Наименование вакансии';
        COMMENT ON COLUMN vacancies.experience 
            IS 'Требуемый опыт';
        COMMENT ON COLUMN vacancies.company 
            IS 'Название компании';
        COMMENT ON COLUMN vacancies.link 
            IS 'Ссылка на вакансию';
        COMMENT ON COLUMN vacancies.salary 
            IS 'Заработная плата';
        COMMENT ON COLUMN vacancies.skills 
            IS 'Ключевые навыки, требуемые в вакансии'; 
        COMMENT ON COLUMN vacancies.description 
            IS 'Описание вакансии';
    """

    conn = get_pg_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()