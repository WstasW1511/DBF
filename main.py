import psycopg2
import datetime

import global_data
from dbf_logger import get_logger
from ftp_loader import walk, ftp_write_to_db, detect_encoding


logger = get_logger(__name__)

DB = global_data.DB

start_time = datetime.datetime.now()

def main():
    logger.info('**************Start Application**************')
    try:
        conn = psycopg2.connect(DB)
        logger.info('Определение кодировок файлов')
        for i in global_data.to_write:
            detect_encoding(i)
        logger.info('Запись файлов в БД')
        for i in range(len(global_data.to_write)):
            ftp_write_to_db(global_data.to_write[i], conn, global_data.encodding_mass[i])
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        logger.error(f"Всего файлов: {len(global_data.to_write)} Всего:{global_data.all_rows} записей.")
        logger.error(f"Затрачено время: {(datetime.datetime.now() - start_time)}")
    finally:
        logger.info(f"Всего файлов: {len(global_data.to_write)} Всего:{global_data.all_rows} записей.")
        logger.info(f"Затрачено время: {(datetime.datetime.now() - start_time)}")
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    walk()
    main()
