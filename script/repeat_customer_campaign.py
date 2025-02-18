from db.connection import get_db_engine_pos,get_db_engine_mre,get_db_engine_wms
import pandas as pd
from db.common_helper import get_data
from db.queries import SALES_REPEAT_QUERY
from db.queries import REPEAT_CUSTOMER_QUERY,SALES_REPEAT_QUERY,ASSURED_QUERY
from services.customer_processing import customer_branded_chronic_purchase,generate_json_data,convert_decimal
from services.sales_processing import sales_processing
from db.common_helper import create_entry
import json
import numpy as np

engine = get_db_engine_pos()
engine_mre = get_db_engine_mre()
engine_wms = get_db_engine_wms()
repeat_customers = get_data(REPEAT_CUSTOMER_QUERY,engine)

list_repeat_customers = list(repeat_customers['customer_id'].unique())

sales_file =  get_data(SALES_REPEAT_QUERY,engine=engine)
sales_file = sales_file[sales_file['customer_id'].isin(list_repeat_customers)]

assured_mapping = get_data(ASSURED_QUERY,engine_wms)

data = customer_branded_chronic_purchase(assured_mapping=assured_mapping,sales=sales_file)

data =sales_processing(data)

repeat_customers['ltv'] = repeat_customers['ltv'].astype('float64')
repeat_customers['customer_id'] = repeat_customers['customer_id'].astype('int64')
repeat_customers['ltv'] = np.round(repeat_customers['ltv'],2)
repeat_customers['loyalty_points'] = repeat_customers['loyalty_points'].astype(int)
repeat_customers['no_of_bills'] = repeat_customers['no_of_bills'].astype('int64')

repeat_customers['campaign_type'] = np.where(
    repeat_customers['customer_id'].isin(data['customer_id'].unique()), 
    'Branded_Chronic', 
    np.where(
        (repeat_customers['no_of_bills'] > 5) & 
        (repeat_customers['ltv'] > 1000) & 
        (repeat_customers['loyalty_points'] > 20), 
        'MSP', 
        np.where(
            (repeat_customers['no_of_bills'] > 5) & 
            (repeat_customers['ltv'] > 1000),  
            'Other', 
            '0'
        )
    )
)

final_df = repeat_customers.merge(data,on='customer_id',how='left')

final_df['products'] = final_df['products'].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else [])


final_df[['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date']] = final_df[
    ['no_of_bills', 'ltv', 'loyalty_points', 'last_purchase_bill_date']
].astype('str')

final_df['json_data'] = final_df.apply(generate_json_data, axis=1)
result_df = final_df[['mobile_number','customer_code','campaign_type','language', 'json_data']]
result_df = result_df.rename(columns={'mobile_number':'customer_mobile'})

result_df = result_df[result_df['campaign_type']!='0']
result_df['campaign'] = 'REPEAT'


create_entry(result_df,'customer_campaigns',engine_mre)
