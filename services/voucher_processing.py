from db.models import GiftVoucher
from db.models import CustomerCampaigns
from db.models import GiftVoucherCode
from db.models import GiftVoucherApplicableStore
from datetime import date, timedelta
from db.common_helper import get_data
from db.connection import get_db_engine_pos
from db.queries import INSERT_GIFT_VOUCHER_STORE_QUERY
from utils.logger import logger, logging
from sqlalchemy.orm.attributes import flag_modified
import json
import random
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
ADMIN_USER = os.getenv('ADMIN_USER')

def generate_voucher_code():
    year = datetime.now().strftime('%y')
    random_number = str(random.randint(0, 999999999)).zfill(9)
    voucher_code = f"{year}G{random_number}"
    return voucher_code


def create_gift_voucher_summary(session_pos, customer_count,voucher_amount,campaign_name,minimum_order_value):
    gift_voucher = GiftVoucher(
        campaign_name=campaign_name,
        voucher_amount=voucher_amount,
        total_vouchers=customer_count,
        expiry_date=str(date.today() + timedelta(8)),
        applicable_type='COCO',
        is_expired=False,
        is_active=True,
        created_by=ADMIN_USER,
        is_minimum_order_value_applicable=True,
        minimum_order_value=minimum_order_value
    )
    session_pos.add(gift_voucher)
    session_pos.flush()
    return gift_voucher.id


def insert_gift_voucher_codes(session_pos, customers, voucher_id):

    for _,customer in customers.iterrows():
        gift_voucher_entry = GiftVoucherCode(
            gift_voucher_id=voucher_id,
            voucher_code=customer['json_data']['voucher_code'],
            voucher_amount=customer['json_data']['voucher_amount'],
            expired_at=date.today() + timedelta(8),
            minimum_order_value=customer['json_data']['minimum_order_value'],
            created_by=ADMIN_USER
        )
        session_pos.add(gift_voucher_entry)


def insert_gift_voucher_stores(session_pos, voucher_id):
    stores = get_data(INSERT_GIFT_VOUCHER_STORE_QUERY, get_db_engine_pos())
    store_ids = list(stores['id'].unique())
    
    for store_id in store_ids:
        store_entry = GiftVoucherApplicableStore(
            gift_voucher_id=int(voucher_id),
            store_id=int(store_id),
            created_by=ADMIN_USER
        )
        session_pos.add(store_entry)