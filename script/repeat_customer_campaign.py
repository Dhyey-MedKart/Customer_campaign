import json
import numpy as np
from datetime import date, timedelta
import pandas as pd
from utils.logger import logging
from db.connection import get_db_engine_pos, get_db_engine_wms,Session_pos,get_db_engine_ecom
from services.voucher_processing import generate_voucher_code, insert_gift_voucher_codes, insert_gift_voucher_stores, create_gift_voucher_summary
from db.common_helper import get_data, create_entry
from db.queries import (
    SALES_REPEAT_QUERY,
    REPEAT_CUSTOMER_QUERY,
    ASSURED_QUERY,
    PRODUCT_MAPPED_DATA
)
from services.generate_savings_url import generate_savings_data_url
from services.customer_processing import (
    customer_branded_chronic_purchase,
    generate_json_data
)
from services.sales_processing import sales_processing


VOUCHER_AMOUNT = 25
MINIMUM_ORDER_VALUE = 500

def initialize_engines():
    try:
        return get_db_engine_pos(), get_db_engine_wms(), get_db_engine_ecom()
    except Exception as e:
        logging()
        raise

def load_mapped_products(engine):
    """
    Load repeat customers from the database and convert the date column.
    """
    try:
        products = get_data(PRODUCT_MAPPED_DATA, engine)
        prod_mapping = dict(zip(products['ws_code'], products['id']))
        return prod_mapping
    except Exception as e:
        logging()
        return {}



def fetch_repeat_customers(engine_pos):
    try:
        repeat_customers = get_data(REPEAT_CUSTOMER_QUERY, engine_pos)
        return repeat_customers, list(repeat_customers['customer_id'].unique())
    except Exception as e:
        logging()
        return pd.DataFrame(), []

def fetch_sales_data(engine_pos, repeat_customer_ids):
    try:
        sales_data = get_data(SALES_REPEAT_QUERY, engine=engine_pos)
        return sales_data[sales_data['customer_id'].isin(repeat_customer_ids)]
    except Exception as e:
        logging()
        return pd.DataFrame()

def process_data(engine_wms, sales_data):
    try:
        assured_mapping = get_data(ASSURED_QUERY, engine_wms)
        data = customer_branded_chronic_purchase(assured_mapping=assured_mapping, sales=sales_data)
        return sales_processing(data)
    except Exception as e:
        logging()
        return pd.DataFrame()

def assign_campaign_types(repeat_customers, processed_data):
    try:
        repeat_customers = repeat_customers.astype({
            'ltv': 'float64',
            'customer_id': 'int64',
            'loyalty_points': 'int64',
            'no_of_bills': 'int64'
        })
        repeat_customers['ltv'] = np.round(repeat_customers['ltv'], 2)

        repeat_customers['campaign_type'] = np.where(
            repeat_customers['customer_id'].isin(processed_data['customer_id'].unique()), 'Branded_Chronic',
            np.where(
                (repeat_customers['no_of_bills'] > 5) & (repeat_customers['ltv'] > 1000) & (repeat_customers['loyalty_points'] > 20), 'MSP',
                np.where(
                    (repeat_customers['no_of_bills'] > 5) & (repeat_customers['ltv'] > 1000), 'Other', '0'
                )
            )
        )
        return repeat_customers
    except Exception as e:
        logging()
        return pd.DataFrame()

def merge_and_prepare_final_df(repeat_customers, processed_data):
    try:
        final_df = repeat_customers.merge(processed_data, on='customer_id', how='left')
        final_df['products'] = final_df['products'].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else []
        )
        final_df[['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date']] = final_df[
            ['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date']
        ].astype(str)
        final_df['json_data'] = final_df.apply(generate_json_data, axis=1)

        ### ADDING THE JSON DATA
        
        final_df.loc[final_df['campaign_type'].isin(['25_RUPEES', 'FREE_OTC']), 'json_data'] = (
            final_df.loc[final_df['campaign_type'].isin(['25_RUPEES', 'FREE_OTC']), 'json_data']
            .apply(lambda x: json.loads(x) if isinstance(x, str) else x)  # Ensure it's a dictionary
            .apply(lambda x: {**x, **{
                'voucher_code': generate_voucher_code(),
                'expiry_date': (date.today() + timedelta(8)).strftime('%d-%b-%Y'),
                'voucher_amount': VOUCHER_AMOUNT,
                'minimum_order_value': MINIMUM_ORDER_VALUE
            }})
        )

        return final_df
    except Exception as e:
        logging()
        return pd.DataFrame()

def prepare_result_df(final_df):
    try:
        result_df = final_df[['mobile_number', 'customer_code', 'campaign_type', 'language', 'json_data']]
        result_df = result_df.rename(columns={'mobile_number': 'customer_mobile'})
        result_df = result_df[result_df['campaign_type'] != '0']
        result_df['campaign'] = 'REPEAT'
        return result_df
    except Exception as e:
        logging()
        return pd.DataFrame()

def main():
    try:
        engine_pos, engine_wms,engine_ecom = initialize_engines()
        repeat_customers, repeat_customer_ids = fetch_repeat_customers(engine_pos)
        sales_data = fetch_sales_data(engine_pos, repeat_customer_ids)
        processed_data = process_data(engine_wms, sales_data)
        repeat_customers = assign_campaign_types(repeat_customers, processed_data)
        final_df = merge_and_prepare_final_df(repeat_customers, processed_data)
        result_df = prepare_result_df(final_df)
        product_mapped_data = load_mapped_products(engine_ecom)
        result_df = generate_savings_data_url(result_df, product_mapped_data)
        result_df.to_csv('repeat_customers.csv')
    except Exception as e:
        logging()

    try:
        session_pos = Session_pos()
        voucher_customers = result_df[result_df['campaign_type'].isin(['FREE_OTC', '25_RUPEES'])]
        if not voucher_customers.empty:
            voucher_customers.loc[:, 'json_data'] = voucher_customers['json_data'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
            voucher_id = create_gift_voucher_summary(session_pos, len(voucher_customers), VOUCHER_AMOUNT, 'FREE_OTC', MINIMUM_ORDER_VALUE)
            insert_gift_voucher_codes(session_pos, voucher_customers, voucher_id)
            insert_gift_voucher_stores(session_pos, voucher_id)
        # CREATE ENTRY
        session_pos.commit()
        #create_entry(result_df, 'customer_campaigns', engine_mre)
    except Exception as e:
        logging()
        session_pos.rollback()
        return

    finally:
        session_pos.close()

    

# if __name__ == "__main__":

main()
