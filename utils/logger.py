import logging
from config.settings import LOG_FILE
import os
import sys


logging.basicConfig(
    filename='file.log', 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def logging():
    exc_type, exc_obj, exc_tb = sys.exc_info()    
    logger.warning("--------------------------------------------------")
    logger.error("Oops! An exception has occured:" +  str(exc_obj))
    if exc_tb is not None:
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("File Name:" + fname)
        logger.error("Line Number:" + str(exc_tb.tb_lineno))
    logger.error("Exception TYPE:" + str(exc_type))
    logger.warning("--------------------------------------------------")