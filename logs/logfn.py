import logging

def log_error(message):
    logging.basicConfig(filename='logs/loginfo.log',level=logging.ERROR,format='%(asctime)s,%(levelname)s,%(message)s')
    logging.error(f'error:{message}')

def log_info(message):
    logging.basicConfig(filename='logs/loginfo.log',level=logging.INFO,format='%(asctime)s,%(levelname)s,%(message)s')
    logging.info(f'{message}')