from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean,DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.settings import conn_string_read_pos

Base = declarative_base()


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




engine_pos = create_engine(conn_string_read_pos())
Base.metadata.create_all(bind=engine_pos)
def create_session_pos():
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_pos)
    return SessionLocal()