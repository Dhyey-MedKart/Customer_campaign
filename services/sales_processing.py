import json
def sales_processing(data):
    data = data.groupby('customer_id').apply(lambda group:json.dumps([
    {
        'bc_product_code': row['bc_product_code'],
        'bc_product_name': row['bc_product_name'],
        'bc_sale_price': row['bc_sales_price'],
        'a_product_code': row['a_product_code'],
        'a_product_name': row['a_product_name'],
        'a_sale_price': row['a_sales_price'],
        'savings' : row['savings']
    } for _, row in group.iterrows()
])).reset_index().rename(columns={0:'products'})

# sales_processing()