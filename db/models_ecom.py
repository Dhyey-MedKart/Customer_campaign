from sqlalchemy import create_engine, Column, String, Integer, DateTime,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.settings import conn_string_ecom

Base = declarative_base()

class CompareSavings(Base):
    __tablename__ = 'compare_savings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    medicine_ids = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)

# Database connection and session
engine_ecom = create_engine(conn_string_ecom())
Base.metadata.create_all(bind=engine_ecom)
def create_session_ecom():
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_ecom)
    return SessionLocal()