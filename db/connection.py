from config.settings import conn_string_ecom,conn_string_mre,conn_string_read_pos,conn_string_read_wms
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker


def get_db_engine_ecom():
    return create_engine(conn_string_ecom())

def get_db_engine_mre():
    return create_engine(conn_string_mre())

def get_db_engine_pos():
    return create_engine(conn_string_read_pos())

def get_db_engine_wms():
    return create_engine(conn_string_read_wms())


# engine_ecom = create_engine(conn_string_ecom())
# Session_ecom = sessionmaker(bind=engine_ecom)
# session_ecom = Session_ecom()

# engine_mre = create_engine(conn_string_mre())
# Session_mre = sessionmaker(bind=engine_mre)
# session_mre = Session_mre()

# engine_pos = create_engine(conn_string_read_pos())
# Session_pos = sessionmaker(bind=engine_pos)
# session_pos = Session_mre()
