from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, Boolean,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.settings import conn_string_mre

Base = declarative_base()

class CustomerCampaigns(Base):
    __tablename__ = 'customer_campaigns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_mobile =  Column(String(100), nullable=False)
    customer_code =  Column(String(100), nullable=False)
    campaign_type = Column(String(100), nullable=False)
    language = Column(String(100), nullable=False)
    json_data = Column(String(100), nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    created_by  = Column(String(100), nullable=True)
    updated_by  = Column(String(100), nullable=True)
    deleted_by  = Column(String(100), nullable=True)
    round = Column(Integer, nullable=False,default=1)
    is_message_sent = Column(Boolean, nullable=False)
    campaign = Column(String(100), nullable=False)
    savings_url = Column(String(250), nullable=True)

class CampaignActivity(Base):
    __tablename__ = 'campaign_activity'

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_code = Column(String(100), nullable=False)
    sub_campaign_name = Column(String(100), nullable=False)
    carrier = Column(String(100), nullable=False, default='AISENSY')
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    campaign_name = Column(String(100), nullable=False)
    customer_mobile = Column(String(15), nullable=False)
    req_json = Column(JSON, nullable=False)
    res_json = Column(JSON, nullable=True)
    json_data = Column(JSON, nullable=False)
    round = Column(Integer, nullable=False)
    status = Column(String(50), nullable=False, default='PENDING')
    language = Column(String(20), nullable=False)


# Database connection and session
engine_mre = create_engine(conn_string_mre())
Base.metadata.create_all(bind=engine_mre)
def create_session_mre():
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_mre)
    return SessionLocal()