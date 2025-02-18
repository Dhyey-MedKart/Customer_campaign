import datetime
from utils.logger import logger, logging
from get_payload_function import get_payload_function
from db.models import create_session, CampaignActivity, CustomerCampaigns
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

    # Prepare the campaign activity entry
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
        # Start the session for database transactions
        session = create_session()
        for _ , row in df.iterrows():
            # Prepare the payload for the campaign

            campaign_entry, payload = message_processer(row)

            # Insert the entry into the database
            session.add(campaign_entry)
            session.commit()

            logger.info(f"Inserted campaign activity for {row['customer_mobile']}")

            # Send the POST request
            response = requests.post(URLX, 
                                    headers={'Content-Type': 'application/json'},
                                    data=payload)
            
            logger.info(f"Response for {row['customer_mobile']}: {response.status_code}")

            # Update the response and status based on the result
            successful_id = row['id']

            session.query(CustomerCampaigns).filter(
                CustomerCampaigns.is_message_sent == False,  # Ensure to use `False` (not `false`)
                CustomerCampaigns.id.in_(successful_id)
            ).update(
                {"is_message_sent": True, "updated_at": func.now()},
                synchronize_session=False  # Or use "fetch" if needed
            )

            # session.execute(text(f'''UPDATE customer_campaigns
            #                     SET is_message_sent = true,
            #                     updated_at = CURRENT_TIMESTAMP
            #                     WHERE is_message_sent IS false
            #                     AND id IN ({successful_id}); '''))
            
            campaign_entry.res_json = response.json()
            campaign_entry.status = 'SUCCESS' if response.status_code == 200 else 'FAILED'
            campaign_entry.updated_at = datetime.now()

            # Commit the changes to the database
            session.commit()

            # Optional: Pause for a short time before processing the next row (for API rate limiting)
            time.sleep(0.1)

        logger.info("All messages sent and campaign entries updated successfully.")
        return True

    except Exception as e:
        session.rollback()
        logging()
        return False
    finally:
        session.close()