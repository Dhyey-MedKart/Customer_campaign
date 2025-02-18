from sqlalchemy.orm import sessionmaker
import logging
import pandas as pd
from sqlalchemy.sql import text


def create_entry(master,table_name,engine):
    try:
        engine = engine
        Session = sessionmaker(bind=engine)
        session = Session()
        master.to_sql(table_name, session.connection(), if_exists="append", index=False)
        session.commit()
        return True
    except Exception as e:
        logging.error(f"There is an error in create entry:{e}")
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
       logging.error(f"Error in get_data_pos: {e} \n Querry: {query}")
       return False
