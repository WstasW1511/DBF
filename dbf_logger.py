import logging
import datetime

def get_logger(name):

    format_logger = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter('\n' + format_logger)
    file_name = datetime.datetime.now().strftime('%Y-%m-%d')

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)

    logging.basicConfig(filename=f'DBF_{str(file_name)}.log', format=format_logger)

    return logger
