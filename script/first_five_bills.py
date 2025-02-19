from db.connection import get_db_engine_pos,get_db_engine_mre, Session_pos
import pandas as pd
from db.common_helper import get_data, create_entry
from db.queries import FIRST_FIVE_BILLS_CUSTOMER_QUERY
from services.customer_processing import convert_decimal
from services.voucher_processing import create_gift_voucher_summary, insert_gift_voucher_codes, insert_gift_voucher_stores
from db.models import GiftVoucher
from utils.logger import logger,logging
from services.voucher_processing import generate_voucher_code
from datetime import date, timedelta

import json
import numpy as np

VOUCHER_AMOUNT = 1
MINIMUM_ORDER_VALUE = 500

def fetch_first_five_bills(engine):
    '''Function to load the data from the database by executing the Query'''
    try:
        first_five_bills = get_data(FIRST_FIVE_BILLS_CUSTOMER_QUERY, engine=engine)
        return first_five_bills

    except Exception as e:
        logging()
        return pd.DataFrame()


def preprocess_data_first_five(first_five_bills):
    '''Function to preprocess the raw first five bills data and add the additional json to the data.'''
    try:
        # Converting the columns to string type
        first_five_bills[first_five_bills.columns] = first_five_bills[first_five_bills.columns].astype('str')

        first_five_bills[first_five_bills.columns] = first_five_bills[first_five_bills.columns].replace('nan','')

        first_five_bills['city'] = first_five_bills['city'].str.capitalize()
        first_five_bills['customer_name'] = first_five_bills['customer_name'].str.upper()

        first_five_bills['free_gift'] = np.where(first_five_bills['campaign_type']=='FREE_OTC','A MOR Z Multivitamin Tablets','')

        first_five_bills['json_data'] = first_five_bills.apply(lambda row: json.dumps({
            'customer_name': row['customer_name'],
            'last_purchase_store_name': row['last_purchase_store_name'],
            'city':row['city'],
            'no_of_bills': row['no_of_bills'],
            'ltv': row['ltv'],
            'loyalty_points': row['loyalty_points'],
            'last_purchase_bill_date': row['last_purchase_bill_date'],
            'free_gift':row['free_gift'],
            'voucher_code': generate_voucher_code(),
            'expiry_date': (date.today() + timedelta(8)).strftime('%d-%b-%Y'),
            'voucher_amount': VOUCHER_AMOUNT,
            'minimum_order_value': MINIMUM_ORDER_VALUE
        }, default=convert_decimal), axis=1)

    except Exception as e:
        logging()
        return first_five_bills


def store_results(first_five_bills):
    '''Function to create a final dataframe with all the required fields'''
    try:
        # Create the final DataFrame with 'mobile_number' and 'json_data'
        result_df = first_five_bills[['mobile_number','customer_code','campaign_type','language', 'json_data']]
        result_df = result_df.rename(columns={'mobile_number':'customer_mobile'})
        result_df = result_df[result_df['campaign_type']!='0']
        result_df['campaign'] = 'FIRST_FIVE_BILLS'
        return result_df
    
    except Exception as e:
        logging()
        return first_five_bills


def first_five_bills_campaign():
    '''Main function to process the first five bills campaign data, store it in the database, and generate gift vouchers and update the three tables all together'''
    try:
        engine = get_db_engine_pos()
        engine_mre = get_db_engine_mre()
        first_five_bills = fetch_first_five_bills(engine)
        first_five_bills = preprocess_data_first_five(first_five_bills)
        result_df = store_results(first_five_bills)
        
        logger.info(f"Added the first five bills campaign data to the database.")

    except Exception as e:
        logging()
        return

    try:
        session_pos = Session_pos()
        voucher_id = create_gift_voucher_summary(session_pos, len(result_df), VOUCHER_AMOUNT, 'FREE_OTC', MINIMUM_ORDER_VALUE)
        insert_gift_voucher_codes(session_pos, result_df, voucher_id)
        insert_gift_voucher_stores(session_pos, voucher_id)
        create_entry(result_df, 'customer_campaigns', engine=engine_mre)
        session_pos.commit()

    except Exception as e:
        logging()
        session_pos.rollback()
        return
    
    finally:
        session_pos.close()