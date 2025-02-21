from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, Boolean,Float,DateTime,Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.settings import conn_string_ecom,conn_string_read_pos

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
    savings_url = Column(String(250), nullable=False)
    voucher_code = Column(String(250), nullable=True)

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
# engine_mre = create_engine(conn_string_mre())
# Base.metadata.create_all(bind=engine_mre)



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

def create_session():
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_mre)
    return SessionLocal()


class GiftVoucher(Base):
    __tablename__ = 'gift_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_name = Column(String(50),nullable=False)
    voucher_amount = Column(Float,nullable=False)
    total_vouchers = Column(Integer,nullable=False)
    expiry_date = Column(DateTime(),nullable=False)
    applicable_type = Column(String(50),nullable=False)
    is_expired = Column(Boolean,default=False)
    is_active = Column(Boolean,default=True)
    created_by =  Column(Integer,nullable=False)
    is_minimum_order_value_applicable =  Column(Boolean,default=False)
    minimum_order_value =  Column(Float,nullable=False,default=0.0)

class GiftVoucherCode(Base):
    __tablename__ = "gift_voucher_codes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gift_voucher_id = Column(Integer, nullable=False)
    utilised_at_store_id = Column(Integer, nullable=True)
    utilised_by_user_id = Column(Integer, nullable=True)
    voucher_code = Column(String, nullable=False)
    voucher_amount = Column(Float, nullable=False)
    utilised_amount = Column(Float, nullable=True, default=0)
    is_expired = Column(Boolean, nullable=True, default=False)
    expired_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=True, default=True)
    created_by = Column(Integer, nullable=False)
    status = Column(String(40), nullable=True, default='AVAILABLE')
    utilised_at_invoice_id = Column(Integer, nullable=True)
    is_minimum_order_value_applicable = Column(Boolean, nullable=True, default=False)
    minimum_order_value = Column(Float, nullable=True, default=0)
    assigned_at_store_id = Column(Integer, nullable=True)
    assigned_at_draft_id = Column(Integer, nullable=True)

# class CompareSavings(Base):
#     __tablename__ = 'compare_savings'

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     medicine_ids = Column(String, nullable=False)
#     created_at = Column(DateTime, nullable=False)
#     deleted_at = Column(DateTime, nullable=True)

class GiftVoucherApplicableStore(Base):
    __tablename__ = "gift_voucher_applicable_stores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gift_voucher_id = Column(Integer, nullable=False)
    store_id = Column(Integer, nullable=False)
    created_by = Column(Integer, nullable=False)
    updated_by = Column(Integer, nullable=True)
    deleted_by = Column(Integer, nullable=True)

