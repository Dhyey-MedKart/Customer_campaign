import json
import warnings
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from uuid import uuid4  # Generate unique savings ID

from db.common_helper import encrypt_id
from utils.logger import logger  # Remove duplicate logging import
from db.models_ecom import CompareSavings
from db.models_ecom import create_session_ecom


def generate_link(customer: dict, pos_product_master_dict: dict):
    """
    Generate and store savings link for a customer.

    Args:
        customer (dict): Customer dictionary with `json_data['subs_products']`.
        pos_product_master_dict (dict): Mapping of `bc_product_code` to `pos_product_id`.

    Returns:
        pd.DataFrame: Updated customer data with `savings_url`.
    """
    session_ecom = create_session_ecom()

    try:
        json_data = json.loads(customer['json_data']) if isinstance(customer['json_data'], str) else customer['json_data']

        subs_products = json_data.get('subs_products', [])
        if not isinstance(subs_products, list) or not subs_products:
            raise ValueError("Invalid or empty 'subs_products' in json_data")

        subs_df = pd.DataFrame(subs_products)

        if 'bc_product_code' not in subs_df.columns:
            raise KeyError("Column 'bc_product_code' not found in subs_products.")

        subs_df['pos_product_id'] = subs_df['bc_product_code'].map(pos_product_master_dict)
        subs_df['encrypt_id'] = subs_df['pos_product_id'].apply(lambda x: encrypt_id(x) if pd.notna(x) else None)

        savings = CompareSavings(
            medicine_ids=str(list(np.unique(subs_df['encrypt_id']))), 
            created_at=datetime.now(),
        )

        session_ecom.add(savings)
        session_ecom.flush() 

        customer_copy = customer.copy()  
        customer_copy['savings_url'] = f"https://www.medkart.in/savings/{encrypt_id(savings.id)}"

        session_ecom.commit()

        return pd.DataFrame([customer_copy])

    except Exception as e:
        session_ecom.rollback()
        logger.error(f"An error occurred in generate_link: {e}")

        # Return customer data with savings_url as None
        customer_data = customer.copy()
        customer_data['savings_url'] = None
        return pd.DataFrame([customer_data])

    finally:
        session_ecom.close()


def generate_savings_data_url(customers, product_mapping):
    """
    Process multiple customers and return a DataFrame with updated savings URLs.

    Args:
        customers (pd.DataFrame): DataFrame of customer dictionaries.
        product_mapping (dict): Mapping of `bc_product_code` to `pos_product_id`.

    Returns:
        pd.DataFrame: Updated customer data.
    """
    updated_customers = []

    for _, customer in customers.iterrows():
        updated_customer_df = generate_link(customer.to_dict(), product_mapping)
        updated_customers.append(updated_customer_df)

    return pd.concat(updated_customers, ignore_index=True) if updated_customers else customers
