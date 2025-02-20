import json
from dotenv import load_dotenv
import os

load_dotenv()

AISENSY_API_KEY = os.getenv("AISENSY_API_KEY")

def get_generic_r1_eng_payload(row):
    """Payload for REPEAT_GENERIC_R1_ENG."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_GENERIC_R1_ENG",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['savings_url']
        ]
    })

def get_generic_r2_hin_payload(row):
    """Payload for REPEAT_GENERIC_R2_HIN."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_GENERIC_R2_HIN",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['savings_url']
        ]
    })

def get_generic_r2_guj_payload(row):
    """Payload for REPEAT_GENERIC_R2_GUJ."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_GENERIC_R2_GUJ",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['savings_url']
        ]
    })

def get_repeat_oth_r1_eng_payload(row):
    """Payload for REPEAT_OTH_R1_ENG."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_OTH_R1_ENG",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', '')
        ]
    })

def get_repeat_oth_r2_hin_payload(row):
    """Payload for REPEAT_OTH_R2_HIN."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_OTH_R2_HIN",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', '')
        ]
    })
       
def generic_replacement_reminder_13_hi_v2(row):
    """Payload for GENERIC_REPLACEMENT_REMINDER_13_HI_V2"""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "GENERIC_REPLACEMENT_REMINDER_13_HI_V2",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['json_data'].get('total_savings', ''),
            row['savings_url']
        ]
    })
    
def generic_replacement_reminder_13_gu_v3(row):
    """Payload for GENERIC_REPLACEMENT_REMINDER_13_GU_V3."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "GENERIC_REPLACEMENT_REMINDER_13_GU_V3",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['json_data'].get('total_savings', ''),
            row['savings_url']
        ]
    })

def generic_replacement_reminder_13(row):
    """Payload for GENERIC_REPLACEMENT_REMINDER_13."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "GENERIC_REPLACEMENT_REMINDER_13",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['json_data'].get('total_savings', ''),
            row['savings_url']
        ]
    })

def get_repeat_oth_r2_guj_payload(row):
    """Payload for REPEAT_OTH_R2_GUJ."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_OTH_R2_GUJ",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', '')
        ]
    })

def get_repeat_msp_r1_eng_payload(row):
    """Payload for REPEAT_MSP_R1_ENG_v2."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_MSP_R1_ENG_v2",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['json_data'].get('loyalty_points', '')
        ]
    })

def get_repeat_msp_r2_hin_payload(row):
    """Payload for REPEAT_MSP_R2_HIN_v1."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_MSP_R2_HIN_v1",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['json_data'].get('loyalty_points', '')
        ]
    })

def get_repeat_msp_r2_guj_payload(row):
    """Payload for REPEAT_MSP_R2_GUJ_v1."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "REPEAT_MSP_R2_GUJ_v1",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            row['json_data'].get('customer_name', ''),
            row['json_data'].get('loyalty_points', '')
        ]
    })
    
def get_first5_msp_invite_r1_eng_payload(row):
    """Payload for OFFER_CAMPAIGN_AUTO_MKT."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "OFFER_CAMPAIGN_AUTO_MKT",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            str(row['json_data'].get('voucher_amount', '')),
            str(row['json_data'].get('voucher_code', '')),
            str(row['json_data'].get('minimum_order_value', '')),
            str(row['json_data'].get('expiry_date', ''))
        ],
        'media': {"url":'https://rp-lp.s3.ap-south-1.amazonaws.com/aisensy-images/25+rs+off.jpg', "filename": "media"}
    })

def get_first5_msp_invite_r2_hi_payload(row):
    """Payload for OFFER_CAMPAIGN_AUTO_MKT."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "OFFER_CAMPAIGN_AUTO_MKT",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            str(row['json_data'].get('voucher_amount', '')),
            str(row['json_data'].get('voucher_code', '')),
            str(row['json_data'].get('minimum_order_value', '')),
            str(row['json_data'].get('expiry_date', ''))
        ],
        'media': {"url":'https://rp-lp.s3.ap-south-1.amazonaws.com/aisensy-images/25+rs+off.jpg', "filename": "media"}
    })

def get_first5_msp_invite_r3_gu_payload(row):
    """Payload for OFFER_CAMPAIGN_AUTO_MKT."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "OFFER_CAMPAIGN_AUTO_MKT",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            str(row['json_data'].get('voucher_amount', '')),
            str(row['json_data'].get('voucher_code', '')),
            str(row['json_data'].get('minimum_order_value', '')),
            str(row['json_data'].get('expiry_date', ''))
        ],
        "media": {"url": 'https://rp-lp.s3.ap-south-1.amazonaws.com/aisensy-images/25+rs+off.jpg', "filename": "media"}
    })

def get_first5_free_otc_invite_r3_eng_payload(row):
    """Payload for FREE_OTC_v1_ENG."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "FREE_OTC_v1_ENG",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            str(row['json_data'].get('customer_name', '')),
            str(row['json_data'].get('free_gift', '')),
            str(row['json_data'].get('minimum_order_value', '')),
            str(row['json_data'].get('voucher_code', '')),
            str(row['json_data'].get('expiry_date', ''))
        ],
        "media": {"url": 'https://rp-lp.s3.ap-south-1.amazonaws.com/aisensy-images/free_gift.jpg', "filename": "media"}
    })

def get_first5_free_otc_invite_r3_hi_payload(row):
    """Payload for FREE_OTC_v1_HI."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "FREE_OTC_v1_HI",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            str(row['json_data'].get('customer_name', '')),
            str(row['json_data'].get('minimum_order_value', '')),
            str(row['json_data'].get('free_gift', '')),
            str(row['json_data'].get('voucher_code', '')),
            str(row['json_data'].get('expiry_date', ''))
        ],
        "media": {"url": 'https://rp-lp.s3.ap-south-1.amazonaws.com/aisensy-images/free_gift.jpg', "filename": "media"}
    })

def get_first5_free_otc_invite_r3_gu_payload(row):
    """Payload for FREE_OTC_v1_HI."""
    return json.dumps({
        "apiKey": AISENSY_API_KEY,
        "campaignName": "FREE_OTC_v1_GU",
        "destination": row['customer_mobile'],
        "userName": row['customer_mobile'],
        "templateParams": [
            str(row['json_data'].get('customer_name', '')),
            str(row['json_data'].get('minimum_order_value', '')),
            str(row['json_data'].get('free_gift', '')),
            str(row['json_data'].get('voucher_code', '')),
            str(row['json_data'].get('expiry_date', ''))
        ],
        "media": {"url": 'https://rp-lp.s3.ap-south-1.amazonaws.com/aisensy-images/free_gift.jpg', "filename": "media"}
    })

def get_payload_function(campaign_name):
    """Returns the corresponding payload function for a given campaign name."""
    payload_functions = {
        "GENERIC_REPLACEMENT_REMINDER_13": generic_replacement_reminder_13,
        "GENERIC_REPLACEMENT_REMINDER_13_HI_V2": generic_replacement_reminder_13_hi_v2,
        "GENERIC_REPLACEMENT_REMINDER_13_GU_V3": generic_replacement_reminder_13_gu_v3,
        "REPEAT_OTH_R1_ENG": get_repeat_oth_r1_eng_payload,
        "REPEAT_OTH_R2_HIN": get_repeat_oth_r2_hin_payload,
        "REPEAT_OTH_R2_GUJ": get_repeat_oth_r2_guj_payload,
        "REPEAT_MSP_R1_ENG_v2": get_repeat_msp_r1_eng_payload,
        "REPEAT_MSP_R2_HIN_v1": get_repeat_msp_r2_hin_payload,
        "REPEAT_MSP_R2_GUJ_v1": get_repeat_msp_r2_guj_payload,
        "OFFER_CAMPAIGN_AUTO_MKT": get_first5_msp_invite_r1_eng_payload,
        "OFFER_CAMPAIGN_AUTO_MKT1": get_first5_msp_invite_r2_hi_payload,
        "OFFER_CAMPAIGN_AUTO_MKT2": get_first5_msp_invite_r3_gu_payload,
        "FREE_OTC_v1_ENG": get_first5_free_otc_invite_r3_eng_payload,
        "FREE_OTC_v1_HI": get_first5_free_otc_invite_r3_hi_payload,
        "FREE_OTC_v1_GU": get_first5_free_otc_invite_r3_gu_payload
    }
    return payload_functions.get(campaign_name)

