import json
import warnings
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from uuid import uuid4  # Generate unique savings ID

from db.common_helper import encrypt_id
from utils.logger import logger  # Remove duplicate logging import
from db.models import CompareSavings
from db.connection import Session_ecom


def generate_link(customer, pos_product_master_dict):
    """
    Generate and store savings link for a customer.

    Args:
        customer (dict): Customer dictionary with `json_data['subs_products']`.
        pos_product_master_dict (dict): Mapping of `bc_product_code` to `pos_product_id`.

    Returns:
        pd.DataFrame: Updated customer data with `savings_url`.
    """
    session_ecom = Session_ecom()

    try:
        # Convert `subs_products` to DataFrame
        json_data = pd.DataFrame(customer['json_data']['subs_products'])

        if 'bc_product_code' in json_data.columns:
            json_data['pos_product_id'] = json_data['bc_product_code'].map(pos_product_master_dict)
            json_data['encrypt_id'] = json_data['pos_product_id'].apply(lambda x: encrypt_id(x) if pd.notna(x) else None)
        else:
            raise KeyError("Column 'bc_product_code' not found in json_data.")

        # Generate savings entry in CompareSavings
        savings = CompareSavings(
            medicine_ids=str(list(np.unique(json_data['encrypt_id']))),  # Fix list formatting
            created_at=datetime.now(),
        )

        session_ecom.add(savings)
        session_ecom.flush()  # Ensure `savings.id` is generated

        # Update customer data with savings URL
        customer_data = customer.copy()  # Copy dictionary to avoid modifying the original
        savings_url = f"https://www.medkart.in/savings/{encrypt_id(savings.id)}"
        customer_data['savings_url'] = savings_url  # Directly add savings_url

        session_ecom.commit()

        # ✅ Fix: Convert to DataFrame properly
        return pd.DataFrame([customer_data])

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
