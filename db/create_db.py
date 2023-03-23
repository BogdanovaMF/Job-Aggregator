from sqlalchemy.orm import declarative_base
from sqlalchemy import Date, String, Column, Integer

Base = declarative_base()

class Vacancy(Base):
    """The class that inherits from the sqlalchemy base class. Create a table"""

    __tablename__ = 'job_data'

    id = Column(Integer, primary_key=True)
    pub_date = Column(Date)
    vacancy = Column(String)
    experience = Column(String)
    company = Column(String)
    link = Column(String)
    salary = Column(String)
    skills = Column(String)
    description = Column(String)





