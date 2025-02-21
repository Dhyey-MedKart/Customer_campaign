import pandas as pd
import numpy as np
import json
from decimal import Decimal
from utils.logger import logging

def customer_branded_chronic_purchase(assured_mapping, sales):
    try:
        # Convert data types
        cols_to_convert = ['bc_mrp', 'bc_package_size', 'a_sales_price', 'bc_sales_price', 'a_mrp', 'a_package_size']
        assured_mapping[cols_to_convert] = assured_mapping[cols_to_convert].astype('float64')
        assured_mapping['bc_product_code'] = assured_mapping['bc_product_code'].astype('int64')
        
        # Filter based on price differences
        assured_mapping = assured_mapping[
            (((assured_mapping['bc_sales_price'] / assured_mapping['bc_package_size']) -
              (assured_mapping['a_sales_price'] / assured_mapping['a_package_size'])) > 1) &
            (((assured_mapping['bc_sales_price'] / assured_mapping['bc_package_size']) -
              (assured_mapping['a_sales_price'] / assured_mapping['a_package_size'])) /
             (assured_mapping['bc_sales_price'] / assured_mapping['bc_package_size']) > 0.2)
        ]

        all_products = list(assured_mapping['bc_product_code'].unique())
        all_products.extend(list(assured_mapping['a_product_code'].unique()))

        required_cols_ass_mapping = assured_mapping[['bc_product_code', 'bc_product_name', 'bc_sales_price',
                                                     'a_product_code', 'a_product_name', 'bc_mrp', 'a_sales_price',
                                                     'bc_package_size', 'a_package_size']]
        
        assured_products = assured_mapping['a_product_code'].unique()
        assured_mapping_dict = dict(zip(assured_mapping['bc_product_code'], assured_mapping['a_product_code']))
        sales = sales[sales['product_code'].isin(all_products)]
        
        filtered_sales_list = []
        # 1039
        for customer, group in sales.groupby('customer_id'):
            print(customer, group)
            customer_products = group['product_code'].tolist()
            bought_assured = [product for product in customer_products if product in assured_products]
            
            if bought_assured:
                excluded_products = {key for key, value in assured_mapping_dict.items() if value in bought_assured}
                filtered_sales = group[(~group['product_code'].isin(excluded_products)) & (~group['product_code'].isin(bought_assured))]
            else:
                filtered_sales = group

            filtered_sales_list.append(filtered_sales)
            
        # If sales are found for a customer
        if filtered_sales_list:  
            print("In")
            data = pd.concat(filtered_sales_list)
            data1 = data.merge(required_cols_ass_mapping, left_on='product_code', right_on='bc_product_code').drop('product_code', axis=1)
            data1['savings'] = ((data1['bc_sales_price'] / data1['bc_package_size']) -
                                (data1['a_sales_price'] / data1['a_package_size'])) * 30
            data1['savings'] = np.round(data1['savings'].astype('float64'), 0)
            data1 = data1.sort_values(by='savings', ascending=False)

            counts_data = data1.groupby(['customer_id']).agg({'savings': 'sum', 'bc_product_name': 'nunique'}).reset_index()
            counts_data = counts_data.rename(columns={'bc_product_name': 'alternate_count', 'savings': 'total_savings'})
            
            data1 = data1.drop_duplicates(subset=['customer_id', 'bc_product_code'])
            data1 = data1[['store_id', 'billdate', 'customer_id', 'bc_product_code', 'bc_product_name', 'bc_sales_price',
                        'a_product_code', 'a_product_name', 'bc_mrp', 'a_sales_price', 'savings']]
            data2 = data1.merge(counts_data, on='customer_id')
            return data2
        print("Out")
        return pd.DataFrame()
    
    except (KeyError, ValueError, TypeError) as e:
        logging()
        return pd.DataFrame() 

def convert_decimal(obj):
    try:
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError("Type not serializable")
    except TypeError as e:
        logging()
        return None

def generate_json_data(row):
    try:
        subs_products = row.get('products', [])
        total_savings = sum(item.get('savings', 0) for item in subs_products) if subs_products else 0
        total_savings = str(total_savings)

        return json.dumps({
            'customer_name': row.get('customer_name', ''),
            'last_purchase_store_name': row.get('last_purchase_store_name', ''),
            'no_of_bills': row.get('no_of_bills', ''),
            'ltv': row.get('ltv', ''),
            'loyalty_points': row.get('loyalty_points', ''),
            'last_purchase_bill_date': row.get('last_purchase_bill_date', ''),
            'subs_products': subs_products,
            'total_savings': total_savings
        }, default=convert_decimal)
    except (KeyError, TypeError, ValueError) as e:
        logging()
        return '{}' 