import pandas as pd
import numpy as np
import json
from decimal import Decimal
def customer_branded_chronic_purchase(assured_mapping,sales):

    assured_mapping[['bc_mrp','bc_package_size','a_sales_price','bc_sales_price']] = assured_mapping[['bc_mrp','bc_package_size','a_sales_price','bc_sales_price']].astype('float64')
    assured_mapping[['a_mrp','a_package_size']] = assured_mapping[['a_mrp','a_package_size']].astype('float64')
    assured_mapping['bc_product_code'] = assured_mapping['bc_product_code'].astype('int64')

    assured_mapping = assured_mapping[(((assured_mapping['bc_sales_price']/assured_mapping['bc_package_size']) - (assured_mapping['a_sales_price']/assured_mapping['a_package_size']))>1) & (((assured_mapping['bc_sales_price']/assured_mapping['bc_package_size']) - (assured_mapping['a_sales_price']/assured_mapping['a_package_size']))/(assured_mapping['bc_sales_price']/assured_mapping['bc_package_size'])>0.2)]

    all_products = list(assured_mapping['bc_product_code'].unique())
    
    all_products.extend(list(assured_mapping['a_product_code'].unique()))
    

    required_cols_ass_mapping = assured_mapping.loc[:,['bc_product_code','bc_product_name','bc_sales_price','a_product_code','a_product_name','bc_mrp','a_sales_price','bc_package_size','a_package_size']]

    assured_products = assured_mapping['a_product_code'].unique()
    assured_mapping = dict(zip(assured_mapping['bc_product_code'],assured_mapping['a_product_code']))
    
    sales = sales[sales['product_code'].isin(all_products)]
    

    filtered_sales_list = []
    for customer, group in sales.groupby('customer_id'):

        customer_products = group['product_code'].tolist()
    

        bought_assured = [product for product in customer_products if product in assured_products]
        

        if bought_assured:
            excluded_products = {key for key, value in assured_mapping.items() if value in bought_assured}
            filtered_sales = group[(~group['product_code'].isin(excluded_products)) & (~group['product_code'].isin(bought_assured))]
        else:
            filtered_sales = group
    
        filtered_sales_list.append(filtered_sales)

    
    data = pd.concat(filtered_sales_list)
    # assured_mapping = assured_mapping.loc[:,['bc_product_code','a_product_code','bc_mrp','a_sales_price','bc_package_size','a_package_size']]
    data1 = data.merge(required_cols_ass_mapping,left_on='product_code',right_on='bc_product_code').drop('product_code',axis=1)
    data1['savings'] = ((data1['bc_sales_price']/data1['bc_package_size'])-(data1['a_sales_price']/data1['a_package_size']))*30
    # data1['savings'] = (data1['bc_sales_price']-data1['a_sales_price'])*30
    data1['bc_sales_price'] = data1['bc_sales_price'].astype('float64')
    data1['a_sale_price/a_packsize*30'] = (data1['a_sales_price']/data1['a_package_size'])*30
    data1['savings'] = np.round(data1['savings'].astype('float64'),0)
    data1 = data1.sort_values(by='savings',ascending=False)
    counts_data = data1.groupby(['customer_id','bc_product_name']).agg({'savings':'min'}).reset_index()
    counts_data = data1.groupby(['customer_id']).agg({'savings':'sum','bc_product_name':'nunique'}).reset_index().rename(columns={'bc_product_name':'alternate_count','savings':'total_savings'})
    
    data1 = data1.drop_duplicates(subset=['customer_id','bc_product_code'])
    data1 = data1.loc[:,['store_id','billdate','customer_id','bc_product_code','bc_product_name','bc_sales_price','a_product_code','a_product_name','bc_mrp','a_sales_price','savings','a_sale_price/a_packsize*30']]
    data2 = data1.merge(counts_data,on='customer_id')
    return data2


def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Type not serializable")

def generate_json_data(row):
    subs_products = row['products']
    total_savings = sum(item.get('savings', 0) for item in subs_products) if subs_products else 0  # Calculate total savings
    total_savings = str(total_savings)
    
    return json.dumps({
        'customer_name': row['customer_name'],
        'last_purchase_store_name': row['last_purchase_store_name'],
        'no_of_bills': row['no_of_bills'],
        'ltv': row['ltv'],
        'loyalty_points': row['loyalty_points'],
        'last_purchase_bill_date': row['last_purchase_bill_date'],
        'subs_products': subs_products,
        'total_savings': total_savings
    }, default=convert_decimal)