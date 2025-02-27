import pandas as pd
import numpy as np
import json
from datetime import datetime
from utils.logger import logging, logger
from db.connection import get_db_engine_pos, get_db_engine_wms,get_db_engine_ecom, get_db_engine_mre
from db.models_pos import create_session_pos
from services.voucher_processing import insert_gift_voucher_codes, insert_gift_voucher_stores, create_gift_voucher_summary
from db.common_helper import get_data, create_entry
from db.queries import (
    get_repeat_sales,
    REPEAT_CUSTOMER_QUERY,
    ASSURED_QUERY,
    PRODUCT_MAPPED_DATA
)
from services.generate_savings_url import generate_savings_data_url
from services.customer_processing import (
    customer_branded_chronic_purchase,
    generate_json_data,
    update_json_data
)
from services.sales_processing import sales_processing


campaign_values = {
    '25_RUPEES': {'voucher_amount': 25,
                  'minimum_order_value': 500},
                  
    'FREE_OTC': {'voucher_amount': 1,
                 'minimum_order_value': 500},

    '': {'voucher_amount':0,
         'minimum_order_value':0}
}

VOUCHER_CAMPAIGNS = ['25_RUPEES', 'FREE_OTC']

def initialize_engines():
    try:
        return get_db_engine_pos(), get_db_engine_wms(), get_db_engine_ecom(), get_db_engine_mre()
    except Exception as e:
        logging()
        return

def fetch_repeat_customers(engine_pos):
    try:
        repeat_customers = get_data(REPEAT_CUSTOMER_QUERY, engine_pos)
        return repeat_customers, repeat_customers['customer_id'].unique().tolist()
    except Exception as e:
        logging()
        return pd.DataFrame(), []

def fetch_sales_data(engine_pos, repeat_customer_ids):
    sales_data = get_data(get_repeat_sales(repeat_customer_ids), engine=engine_pos)
    return sales_data


def process_data(engine_wms, sales_data):
    assured_mapping = get_data(ASSURED_QUERY, engine_wms)
    data = customer_branded_chronic_purchase(assured_mapping=assured_mapping, sales=sales_data)
    return sales_processing(data)


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
        if not processed_data.empty:
            final_df = repeat_customers.merge(processed_data, on='customer_id', how='left')
            final_df['products'] = final_df['products'].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else []
            )
        else:
            final_df = repeat_customers

        final_df[['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date']] = final_df[
            ['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date']
        ].astype(str)
        final_df['json_data'] = final_df.apply(generate_json_data, axis=1)
        ### ADDING THE JSON DATA
        campaign_mask = final_df['campaign_type'].isin(VOUCHER_CAMPAIGNS)
        final_df.loc[campaign_mask, 'json_data'] = final_df.loc[campaign_mask].apply(
            lambda row: update_json_data(row['json_data'], row['campaign_type'], campaign_values), axis=1
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

def main():
    try:
        engine_pos, engine_wms,engine_ecom,engine_mre = initialize_engines()
        repeat_customers, repeat_customer_ids = fetch_repeat_customers(engine_pos)
        if len(repeat_customer_ids) == 0:
            raise Exception('No customers...')
        sales_data = fetch_sales_data(engine_pos, tuple(repeat_customer_ids))
        processed_data = process_data(engine_wms, sales_data)
        if processed_data.empty:
            logger.info('No Branded Chronic customers')
        repeat_customers = assign_campaign_types(repeat_customers, processed_data)
        final_df = merge_and_prepare_final_df(repeat_customers, processed_data)
        result_df = prepare_result_df(final_df)
        product_mapped_data = load_mapped_products(engine_ecom)
        result_df = generate_savings_data_url(result_df, product_mapped_data)
        
        try:
            session_pos = create_session_pos()
            voucher_customers = result_df[result_df['campaign_type'].isin(VOUCHER_CAMPAIGNS)]
            if not voucher_customers.empty:
                voucher_id = []
                voucher_customers.loc[:, 'json_data'] = voucher_customers['json_data'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                for type in campaign_values:
                    campaign_voucher_customers = voucher_customers[voucher_customers['campaign_type']==type]
                    if not campaign_voucher_customers.empty:
                        voucher_id = create_gift_voucher_summary(session_pos, len(campaign_voucher_customers), campaign_values[type]['voucher_amount'],type,campaign_values[type]['minimum_order_value'])
                        insert_gift_voucher_codes(session_pos, campaign_voucher_customers, voucher_id)
                        insert_gift_voucher_stores(session_pos, voucher_id)

            
            result_df.loc[:, 'json_data'] = result_df['json_data'].apply(lambda x: x if isinstance(x, str) else json.dumps(x))
            result_df = result_df[result_df['customer_code'].notna()]

            # CREATE ENTRY
            if create_entry(result_df, 'customer_campaigns', engine_mre):
                logger.info(f'Succesfully inserted data of Repeat_Customers campaign at {datetime.now()}')
            else:
                raise Exception
        except Exception as e:
            logging()
            session_pos.rollback()
            return

        finally:
            session_pos.commit()
            session_pos.close()

    except Exception as e:
        logging()



    

# if __name__ == "__main__":

main()
logger.info(f'Executed Repeat_Customers at {datetime.now()}')
