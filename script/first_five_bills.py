from db.connection import get_db_engine_pos, get_db_engine_mre
from db.models_pos import create_session_pos
from db.common_helper import get_data, create_entry
from db.queries import FIRST_FIVE_BILLS_CUSTOMER_QUERY
from services.customer_processing import generate_json_data,update_json_data
from services.voucher_processing import create_gift_voucher_summary, insert_gift_voucher_codes, insert_gift_voucher_stores
from utils.logger import logger,logging
from services.voucher_processing import create_gift_voucher_summary, insert_gift_voucher_codes, insert_gift_voucher_stores
from datetime import date, datetime
import json
import numpy as np

campaign_values = {
    '25_RUPEES': {'voucher_amount': 25,
                  'minimum_order_value': 500},
                  
    'FREE_OTC': {'voucher_amount': 1,
                 'minimum_order_value': 500},

    '': {'voucher_amount':0,
         'minimum_order_value':0}
}

VOUCHER_CAMPAIGNS = ['25_RUPEES', 'FREE_OTC']

def first_five_bills_campaign():
    engine = get_db_engine_pos()
    first_five_bills = get_data(FIRST_FIVE_BILLS_CUSTOMER_QUERY, engine=engine)
    first_five_bills = first_five_bills[first_five_bills['campaign_type']!='0']
    return first_five_bills


def preprocess_data_first_five(first_five_bills):
    '''Function to preprocess the raw first five bills data and add the additional json to the data.'''
    try:
        # Converting the columns to string type
        first_five_bills[first_five_bills.columns] = first_five_bills[first_five_bills.columns].astype('str')

        first_five_bills[first_five_bills.columns] = first_five_bills[first_five_bills.columns].replace('nan','')

        first_five_bills['city'] = first_five_bills['city'].str.capitalize()
        first_five_bills['customer_name'] = first_five_bills['customer_name'].str.upper()

        first_five_bills['free_gift'] = np.where(first_five_bills['campaign_type']=='FREE_OTC','A MOR Z Multivitamin Tablets','')
        first_five_bills['json_data'] = first_five_bills.apply(generate_json_data, axis=1)
        
        return first_five_bills
    except Exception as e:
        logging()
        return first_five_bills


def store_results(first_five_bills):
    '''Function to create a final dataframe with all the required fields'''
    try:
        # Create the final DataFrame with 'mobile_number' and 'json_data'
        result_df = first_five_bills[['mobile_number','customer_code','campaign_type','language', 'json_data']]
        result_df = result_df.rename(columns={'mobile_number':'customer_mobile'})
        result_df['campaign'] = 'FIRST_FIVE_BILLS'
        campaign_mask = result_df['campaign_type'].isin(['25_RUPEES', 'FREE_OTC'])
        result_df.loc[campaign_mask, 'json_data'] = result_df.loc[campaign_mask].apply(
            lambda row: update_json_data(row['json_data'], row['campaign_type'], campaign_values), axis=1
        )
        result_df.loc[result_df['campaign_type'].isin(['25_RUPEES', 'FREE_OTC']), 'json_data'] = result_df.loc[result_df['campaign_type'].isin(['25_RUPEES', 'FREE_OTC']), 'json_data'].apply(lambda x : json.dumps(x))
    
        try:
            session_pos = create_session_pos()
            engine_mre = get_db_engine_mre()
            voucher_customers = result_df[result_df['campaign_type'].isin(VOUCHER_CAMPAIGNS)]
            if not voucher_customers.empty:
                voucher_customers.loc[:, 'json_data'] = voucher_customers['json_data'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
                for type in campaign_values:
                    campaign_voucher_customers = voucher_customers[voucher_customers['campaign_type']==type]
                    if not campaign_voucher_customers.empty:
                        voucher_id = create_gift_voucher_summary(session_pos, len(campaign_voucher_customers), campaign_values[type]['voucher_amount'],type,campaign_values[type]['minimum_order_value'])
                        insert_gift_voucher_codes(session_pos, campaign_voucher_customers, voucher_id)
                        insert_gift_voucher_stores(session_pos, voucher_id)
            else:
                logger.info(f"No data to insert in campaign first five bills on {format(date.today(),'%d-%b-%Y')}")
            # result_df.to_csv("fis.csv")
            result_df = result_df[result_df['customer_code'].notna()]
            if create_entry(result_df, 'customer_campaigns', engine=engine_mre):
                print('First_five_bills data inserted successfully...')
            else:
                raise Exception
        except Exception as e:
            session_pos.rollback()
            logging()
            return
        
        finally:
            session_pos.commit()
            session_pos.close()
            
    except Exception as e:
        logging()
        return 


def main():
    df = first_five_bills_campaign()
    if not df.empty:
        df = preprocess_data_first_five(df)
        store_results(df)
    else:
        logger.info(f"No data available for campaign first five bills on {format(date.today(),'%d-%b-%Y')}")
    

main()
print(f'Succesfully executed First_five_bills at {datetime.now()}')
logger.info(f'Succesfully executed First_five_bills at {datetime.now()}')