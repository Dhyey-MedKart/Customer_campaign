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
from db.connection import create_session_ecom


def generate_link(customer, pos_product_master_dict):
    """
    Generate and store savings link for a customer.

    Args:
        customer (dict): Customer dictionary with `json_data['subs_products']`.
        pos_product_master_dict (dict): Mapping of `bc_product_code` to `pos_product_id`.

    Returns:
        dict: Updated customer dictionary with `savings_url`.
    """
    session_ecom = create_session_ecom()

    try:
        # Convert `subs_products` to DataFrame
        json_data = pd.DataFrame(customer['json_data']['subs_products'])
        json_data['pos_product_id'] = json_data['bc_product_code'].map(pos_product_master_dict)
        json_data['encrypt_id'] = json_data['pos_product_id'].apply(lambda x: encrypt_id(x) if pd.notna(x) else None)

        # Generate savings entry in CompareSavings
        savings = CompareSavings(
            medicine_ids=str(list(np.unique(json_data['encrypt_id']))),  # Fix list formatting
            created_at=datetime.now(),
        )

        session_ecom.add(savings)
        session_ecom.flush()  # Ensure `savings.id` is generated

        # Update customer data with savings URL
        savings_url = f"https://www.medkart.in/savings/{encrypt_id(savings.id)}"
        customer.update({'savings_url': savings_url})

        session_ecom.commit()
        return customer  

    except Exception as e:
        session_ecom.rollback()
        logger.error(f"An error occurred in generate_link: {e}")
        return False  

    finally:
        session_ecom.close()


def generate_savings_data_url(customers, product_mapping):
    """
    Process multiple customers and return a DataFrame with updated savings URLs.

    Args:
        customers (list): List of customer dictionaries.
        product_mapping (dict): Mapping of `bc_product_code` to `pos_product_id`.

    Returns:
        pd.DataFrame: Updated customer data.
    """
    updated_customers = []

    for customer in customers:
        updated_customer = generate_link(customer, product_mapping)
        if updated_customer: 
            updated_customers.append(updated_customer)

    return pd.DataFrame(updated_customers) if updated_customers else pd.DataFrame()
