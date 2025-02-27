from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import warnings
from connections import get_db_engine_mre
from db.queries import DELETE_MSG_SENT_QUERY, UPDATE_ROUNDS_QUERY
from utils.logger import logging

warnings.filterwarnings("ignore")


def update_shoot_round():
    try:
        Session = sessionmaker(bind=get_db_engine_mre())
        session = Session()

        session.execute(text(DELETE_MSG_SENT_QUERY))
        session.execute(text(UPDATE_ROUNDS_QUERY))

        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logging()
        return False
    finally:
        session.close()

