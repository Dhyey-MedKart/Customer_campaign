import os
from dotenv import load_dotenv

load_dotenv()
LOG_FILE = os.getenv('LOG_FILE')    
DB_WMS_HOST = os.getenv("DB_WMS_READ_HOST")
DB_WMS_PORT = os.getenv("DB_WMS_PORT")
DB_WMS_NAME = os.getenv("DB_WMS_NAME")
DB_WMS_USER = os.getenv("DB_WMS_USER")
DB_WMS_PASS = os.getenv("DB_WMS_PASS")

DB_MRE_HOST = os.getenv("DB_MRE_READ_HOST")
DB_MRE_PORT = os.getenv("DB_MRE_PORT")
DB_MRE_NAME = os.getenv("DB_MRE_NAME")
DB_MRE_USER = os.getenv("DB_MRE_USER")
DB_MRE_PASS = os.getenv("DB_MRE_PASS")

DB_POS_HOST = os.getenv("DB_POS_READ_HOST")
DB_POS_PORT = os.getenv("DB_POS_PORT")
DB_POS_NAME = os.getenv("DB_POS_NAME")
DB_POS_USER = os.getenv("DB_POS_USER")
DB_POS_PASS = os.getenv("DB_POS_PASS")

DB_ECOM_HOST = os.getenv("DB_ECOM_READ_HOST")
DB_ECOM_PORT = os.getenv("DB_ECOM_PORT")
DB_ECOM_NAME = os.getenv("DB_ECOM_NAME")
DB_ECOM_USER = os.getenv("DB_ECOM_USER")
DB_ECOM_PASS = os.getenv("DB_ECOM_PASS")


def conn_string_mre():
    return f"postgresql://{DB_MRE_USER}:{DB_MRE_PASS}@{DB_MRE_HOST}/{DB_MRE_NAME}"

def conn_string_ecom():
    return f"postgresql://{DB_ECOM_USER}:{DB_ECOM_PASS}@{DB_ECOM_HOST}/{DB_ECOM_NAME}"

def conn_string_read_wms():
    return f"postgresql://{DB_WMS_USER}:{DB_WMS_PASS}@{DB_WMS_HOST}/{DB_WMS_NAME}"

def conn_string_read_pos():
    return f"postgresql://{DB_POS_USER}:{DB_POS_PASS}@{DB_POS_HOST}/{DB_POS_NAME}"

