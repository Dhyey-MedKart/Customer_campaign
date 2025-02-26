import json
from datetime import datetime
import sys
from utils.logger import logging, logger
import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
from db.connection import get_db_engine_pos, get_db_engine_wms, get_db_engine_ecom,get_db_engine_mre
from db.models_pos import create_session_pos
from services.voucher_processing import create_gift_voucher_summary, insert_gift_voucher_codes, insert_gift_voucher_stores
from db.common_helper import get_data, create_entry
from db.queries import LOST_CUSTOMER_QUERY, get_lost_customer_sales_query, ASSURED_QUERY, PRODUCT_MAPPED_DATA
from services.customer_processing import (
    customer_branded_chronic_purchase,
    generate_json_data,
    update_json_data
)
from services.generate_savings_url import generate_savings_data_url
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
        raise
def load_customers(engine):
    """
    Load repeat customers from the database and convert the date column.
    """
    customers = get_data(LOST_CUSTOMER_QUERY, engine)
    customers['last_purchase_bill_date'] = pd.to_datetime(customers['last_purchase_bill_date'])
    return customers


def compute_reference_date(today):
    """
    Compute the reference date based on the current day.
    """
    try:
        if 1 <= today.day <= 14:
            return today.replace(day=1)
        else:
            return today.replace(day=14 if today.month == 2 else 15)
    except Exception as e:
        logging()
        return today

def apply_campaign_category(df, reference_date, campaign_name='LOST_CUSTOMER'):
    """
    Categorize customers based on their last purchase date relative to the reference date.
    """
    try:
        if campaign_name == 'LOST_CUSTOMER':
            df['category'] = 'unknown'
            df.loc[
                (df['last_purchase_bill_date'] >= reference_date - DateOffset(months=5, days=15)) &
                (df['last_purchase_bill_date'] < reference_date - DateOffset(months=5)),
                'category'
            ] = 'M4'
            df.loc[
                (df['last_purchase_bill_date'] >= reference_date - DateOffset(months=5)) &
                (df['last_purchase_bill_date'] < reference_date - DateOffset(months=4, days=15)),
                'category'
            ] = 'M3'
            df.loc[
                (df['last_purchase_bill_date'] >= reference_date - DateOffset(months=4, days=15)) &
                (df['last_purchase_bill_date'] < reference_date - DateOffset(months=4)),
                'category'
            ] = 'M2'
            df.loc[
                (df['last_purchase_bill_date'] >= reference_date - DateOffset(months=4)) &
                (df['last_purchase_bill_date'] < reference_date - DateOffset(months=3, days=15)),
                'category'
            ] = 'M1'
        return df[df['category'] != 'unknown']
    except Exception as e:
        logging()
        return pd.DataFrame()

def load_sales_data(engine, customer_ids, reference_date):
    """
    Retrieve the sales data for the given customer IDs and reference date.
    """
    query = get_lost_customer_sales_query(customer_ids=tuple(customer_ids), reference_date=reference_date)
    sales_data = get_data(query, engine)
    return sales_data


def process_sales_data(assured_mapping, sales_data):
    """
    Process the sales data by applying business rules.
    """
    try:
        processed_data = customer_branded_chronic_purchase(assured_mapping=assured_mapping, sales=sales_data)
        processed_data = sales_processing(processed_data)
        return processed_data
    except Exception as e:
        logging()
        return pd.DataFrame()

def assign_campaign_types(customers, sales_data):
    """
    Determine campaign type based on customer category, loyalty points, and sales data.
    """
    try:
        customers['ltv'] = customers['ltv'].astype('float64').round(2)
        customers['customer_id'] = customers['customer_id'].astype('int64')
        customers['loyalty_points'] = customers['loyalty_points'].astype(int)
        customers['no_of_bills'] = customers['no_of_bills'].astype('int64')

        conditions = [
            (customers['category'] == 'M1'),
            (customers['category'] == 'M4'),
            ((customers['loyalty_points'] > 20) & (customers['category'] == 'M2')),
            ((customers['loyalty_points'] <= 20) & (customers['category'] == 'M2')),
            ((customers['customer_id'].isin(sales_data['customer_id'].unique())) & (customers['category'] == 'M3')),
            ((~customers['customer_id'].isin(sales_data['customer_id'].unique())) & (customers['category'] == 'M3'))
        ]

        choices = ['25_RUPEES', '25_RUPEES', 'MSP', '17_PERCENT', 'Branded_Chronic', 'FREE_HD']

        customers['campaign_type'] = np.select(conditions, choices, default='None')
        return customers
    except Exception as e:
        logging()
        return pd.DataFrame()

<<<<<<< HEAD
def build_final_dataframe(customers, sales_data, reference_date):
=======

def build_final_dataframe(customers, sales_data):
>>>>>>> 8f8a707138728c396458722619c8de5e7daa1912
    """
    Merge customer and sales data, transform fields, and prepare the final result DataFrame.
    """
    try:
        if not sales_data.empty:
            merged_df = customers.merge(sales_data, on='customer_id', how='left')
            merged_df['products'] = merged_df['products'].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else []
            )
        else:
            merged_df = customers

        fields_to_convert = ['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date','store_contact','city']
        merged_df[fields_to_convert] = merged_df[fields_to_convert].astype('str')

        # Create JSON
        merged_df['json_data'] = merged_df.apply(generate_json_data, axis=1)

        ## ADDING THE EXTRA JSON DATA 
        campaign_mask = merged_df['campaign_type'].isin(['25_RUPEES', 'FREE_OTC'])
        merged_df.loc[campaign_mask, 'json_data'] = merged_df.loc[campaign_mask].apply(
            lambda row: update_json_data(row['json_data'], row['campaign_type'], campaign_values), axis=1
        )
        
        result_df = merged_df[['mobile_number', 'customer_code', 'campaign_type', 'language', 'json_data']].copy()
        result_df.rename(columns={'mobile_number': 'customer_mobile'}, inplace=True)

        result_df = result_df[result_df['campaign_type'] != '0']
        result_df['campaign'] = 'LOST'
        
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
        return json.dumps({})

def main():
    try:
        engine_pos, engine_wms, engine_ecom, engine_mre = initialize_engines()
        customers = load_customers(engine_pos)
        if customers.empty:
            raise Exception("No customers in lost customers")
        today = datetime.today()
        reference_date = compute_reference_date(today)
        customers['reference_date'] = reference_date
        
        customers = apply_campaign_category(customers, reference_date, campaign_name='LOST_CUSTOMER')
        
        m3_customer_ids = customers[customers['category'] == 'M3']['customer_id'].tolist()
        if len(m3_customer_ids) == 0:
            processed_sales = pd.DataFrame(columns=['customer_id','category'])
        else:
            sales_data = load_sales_data(engine_pos, m3_customer_ids, reference_date)
            assured_mapping = get_data(ASSURED_QUERY, engine_wms)
            processed_sales = process_sales_data(assured_mapping, sales_data)

        customers = assign_campaign_types(customers, processed_sales)
        final_df = build_final_dataframe(customers, processed_sales)
        
        # URL parameter
        product_mapped_data = load_mapped_products(engine_ecom)
        final_df = generate_savings_data_url(final_df, product_mapped_data)
        

        try:
            session_pos = create_session_pos()
            voucher_customers = final_df[final_df['campaign_type'].isin(VOUCHER_CAMPAIGNS)]
            if not voucher_customers.empty:
                voucher_id = []
                voucher_customers.loc[:, 'json_data'] = voucher_customers['json_data'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                for type in campaign_values:
                    campaign_voucher_customers = voucher_customers[voucher_customers['campaign_type']==type]
                    if not campaign_voucher_customers.empty:
                        voucher_id = create_gift_voucher_summary(session_pos, len(campaign_voucher_customers), campaign_values[type]['voucher_amount'],type,campaign_values[type]['minimum_order_value'])
                        insert_gift_voucher_codes(session_pos, campaign_voucher_customers, voucher_id)
                        insert_gift_voucher_stores(session_pos, voucher_id)

            # CREATE ENTRY
        
            final_df['json_data'] = final_df['json_data'].apply(lambda x:json.dumps(x))
            final_df = final_df[final_df['customer_code'].notna()]
            create_entry(final_df, 'customer_campaigns', engine_mre)


        except Exception as e:
            logging()
            session_pos.rollback()
            return

        finally:
            session_pos.commit()
            session_pos.close()
        
    except Exception as e:
        logging()


today = datetime.today().day
if today not in [5, 20,25]:
    logger.error("Not execuable today")
    sys.exit()
main()
<<<<<<< HEAD
=======
print(f'Succesfully executed Lost_customers at {datetime.now()}')
logger.info(f'Succesfully executed Lost_customers at {datetime.now()}')
>>>>>>> 8f8a707138728c396458722619c8de5e7daa1912

