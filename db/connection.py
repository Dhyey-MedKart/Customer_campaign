from config.settings import conn_string_ecom,conn_string_read_pos,conn_string_read_wms,conn_string_mre
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


