import datetime
from utils.logger import logger, logging
from services.generate_payloads import get_payload_function
from db.models_mre import create_session_mre, CampaignActivity, CustomerCampaigns
import requests
from sqlalchemy import text, func
import time
import os
from dotenv import load_dotenv

load_dotenv()

URLX = os.getenv('MSG_POST_URL')    

def message_processer(row):
    payload_function = get_payload_function(row['campaign_name'])
    if payload_function:
        payload = payload_function(row)
    else:
        logger.warning(f"No payload function found for campaign: {row['campaign_name']}")

    campaign_entry = CampaignActivity(
        customer_code=row['customer_code'],
        sub_campaign_name=row['campaign_name'],
        campaign_name=row['campaign'],
        customer_mobile=row['customer_mobile'],
        req_json=payload,
        json_data=row['json_data'],
        round=row['round'],
        created_at=datetime.now(),
        updated_at=datetime.now(),
        language= 'ENGLISH' if row['round'] == 'first' else row['language'],
        status='PENDING',
        carrier='AISENSY'
    )

    return campaign_entry, payload

def send_message(df):
    try:
        session = create_session_mre()
        for _ , row in df.iterrows():

            campaign_entry, payload = message_processer(row)

            session.add(campaign_entry)
            session.commit()

            logger.info(f"Inserted campaign activity for {row['customer_mobile']}")

            response = requests.post(URLX, 
                                    headers={'Content-Type': 'application/json'},
                                    data=payload)
            
            logger.info(f"Response for {row['customer_mobile']}: {response.status_code}")

            successful_id = row['id']

            session.query(CustomerCampaigns).filter(
                CustomerCampaigns.is_message_sent == False,
                CustomerCampaigns.id.in_(successful_id)
            ).update(
                {"is_message_sent": True, "updated_at": func.now()},
                synchronize_session=False
            )

            # session.execute(text(f'''UPDATE customer_campaigns
            #                     SET is_message_sent = true,
            #                     updated_at = CURRENT_TIMESTAMP
            #                     WHERE is_message_sent IS false
            #                     AND id IN ({successful_id}); '''))
            
            campaign_entry.res_json = response.json()
            campaign_entry.status = 'SUCCESS' if response.status_code == 200 else 'FAILED'
            campaign_entry.updated_at = datetime.now()

            session.commit()
            time.sleep(0.1)

        logger.info("All messages sent and campaign entries updated successfully.")
        return True

    except Exception as e:
        session.rollback()
        logging()
        return False
    finally:
        session.close()