import sys
from shoot.messager import send_message
from services.campaign_mappings import map_campaign
from db.queries import get_customer_campaign_data
from db.common_helper import get_data
from db.connection import get_db_engine_mre
from utils.logger import logging
def main():
    try:
        engine = get_db_engine_mre()
        input_round = sys.argv[1]

        if input_round not in ['first', 'second']:
            print("Invalid input. Please provide either 'first' or 'second'.")
            raise ValueError("Invalid input. Please provide either 'first' or 'second'.")

        round = 1 if input_round == 'first' else 2

        df = get_data(get_customer_campaign_data,engine)
        df = df[~((df['campaign_type'] == 'Branded_Chronic') & df['savings_url'].isnull())]
        
        df = map_campaign(df, input_round)
        df['round'] = input_round
        
        if send_message(df):
            print('Done')

    except Exception as e:    
        logging()

if __name__ == '__main__':
    main()
