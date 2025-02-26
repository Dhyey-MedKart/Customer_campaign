def main():
    # import script.first_five_bills
    import script.lost_customers

    # import script.repeat_customer_campaign
    from utils.logger import logger
    from datetime import datetime
    print(f'Succesfully executed scripts at {datetime.now()}')
    logger.info(f'Succesfully executed scripts at {datetime.now()}')

if __name__=='__main__':
    main()
