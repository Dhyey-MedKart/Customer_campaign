from db.connection import get_db_engine_pos,get_db_engine_mre
import pandas as pd
from db.common_helper import get_data, create_entry
from db.queries import FIRST_FIVE_BILLS_CUSTOMER_QUERY
from services.customer_processing import convert_decimal
from utils.logger import logger,logging
import json
import numpy as np
def first_five_bills_campaign():
    try:
        engine = get_db_engine_pos()
        engine_mre = get_db_engine_mre()
    except Exception as e:
        logging()
        return
    first_five_bills = get_data(FIRST_FIVE_BILLS_CUSTOMER_QUERY, engine=engine)
    try:
        # Converting the columns to string type
        first_five_bills[first_five_bills.columns] = first_five_bills[first_five_bills.columns].astype('str')
        # Replacing the nan values with an empty string, then capitalizing the city column followed by converting the customer_name to uppercase
        first_five_bills[first_five_bills.columns] = first_five_bills[first_five_bills.columns].replace('nan','')
        first_five_bills['city'] = first_five_bills['city'].str.capitalize()
        first_five_bills['customer_name'] = first_five_bills['customer_name'].str.upper()
        first_five_bills.to_csv('first_five_bills.csv',index=False)
        # Creating a new column free_gift and assigning the value 'A MOR Z Multivitamin Tablets' if the campaign_type is 'FREE_OTC' else an empty string
        first_five_bills['free_gift'] = np.where(first_five_bills['campaign_type']=='FREE_OTC','A MOR Z Multivitamin Tablets','')
        # Create a new column for JSON data, excluding 'mobile_number'
        first_five_bills['json_data'] = first_five_bills.apply(lambda row: json.dumps({
            'customer_name': row['customer_name'],
            'last_purchase_store_name': row['last_purchase_store_name'],
            'city':row['city'],
            'no_of_bills': row['no_of_bills'],
            'ltv': row['ltv'],
            'loyalty_points': row['loyalty_points'],
            'last_purchase_bill_date': row['last_purchase_bill_date'],
            'free_gift':row['free_gift']
        }, default=convert_decimal), axis=1)
        
        # Create the final DataFrame with 'mobile_number' and 'json_data'
        result_df = first_five_bills[['mobile_number','customer_code','campaign_type','language', 'json_data']]
        result_df = result_df.rename(columns={'mobile_number':'customer_mobile'})
        result_df = result_df[result_df['campaign_type']!='0']
        result_df['campaign'] = 'FIRST_FIVE_BILLS'
        logger.info(f"Data transformation for first five bills campaign completed")
    
    except Exception as e:
        logging()
    # Insert the data into the database
    # create_entry(result_df, 'customer_campaigns', engine=engine_mre)