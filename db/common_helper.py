from sqlalchemy.orm import sessionmaker
from utils.logger import logging,logger
import pandas as pd
from sqlalchemy.sql import text


def create_entry(master,table_name,engine):
    try:
        engine = engine
        Session = sessionmaker(bind=engine)
        session = Session()
        master.to_sql(table_name, session.connection(), if_exists="append", index=False)
        session.commit()
        logger.info(f"Successfully created entry for {table_name}")
        return True
    except Exception as e:
        logging()
        session.rollback()
        return False

def get_data(query,engine):
    try:
        engine_mre_read = engine
        with engine_mre_read.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
            return df
    except Exception as e:
       logging()
       return False
