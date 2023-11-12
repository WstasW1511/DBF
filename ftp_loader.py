import json
from collections import UserDict
import os
import global_data
import ftputil
import psycopg2
from dbfread import DBF
from ftptool import FTPHost
from tqdm import tqdm
from dbf_logger import get_logger
from chardet.universaldetector import UniversalDetector


logger = get_logger(__name__)



def create_table(file,conn):
    table_name = file.split('/')[-1].lower().replace('.dbf',' ').strip()
    table_name = str(global_data.prefix) + table_name
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}(id SERIAL PRIMARY KEY, blob jsonb)")
        conn.commit()
        logger.info(f'Создана таблица: {table_name}')
        return table_name
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)

def walk():
    try:
        conn_ftp = FTPHost.connect(str(global_data.ftp_host), user=str(global_data.ftp_user),
                                 password=str(global_data.ftp_password))
        logger.info('Connecting to FTP success!')
        path = conn_ftp.current_directory = ".."
        # path = path + '/2023/1'
        for (dirname, subdirs, files) in conn_ftp.walk(path):
            for name in files:
                if name.lower().endswith(".dbf"):
                    d = dirname +"/"+ name
                    global_data.to_write.append(d)
        conn_ftp.quit()
        return
    except Exception as exc:
        logger.error("Ошибка:\n", exc)
        return

def detect_encoding(filename):
    try:
        file_name = filename.split('/')[-1]
        logger.info(f'Определение кодировки для: {file_name}')
        with ftputil.FTPHost(str(global_data.ftp_host), str(global_data.ftp_user), str(global_data.ftp_password)) as host:
            with host.open(filename, 'rb') as f:
                detector = UniversalDetector()
                for line in tqdm(f.readlines(), ascii=False, desc='Выполняется определение кодировки'):
                    detector.feed(line)
                    if detector.done:
                        break
                detector.close()
                encoding = detector.result['encoding']
                if encoding:
                    global_data.encodding_mass.append(encoding)
                    logger.info(f'Кодировка для файла {file_name} определена: {str(encoding)}')
                    return
                else:
                    global_data.encodding_mass.append('')
                    logger.warning(f'Не удалось определить кодировку для файла {file_name}')
                    return
    except FileNotFoundError:
        logger.warning("File not found")
        host.close()
        return
    except Exception as exc:
        logger.error(f"An exception of type {type(exc).__name__} occurred.")
        return


def ftp_write_to_db(file,  conn, encoding=''):

    with ftputil.FTPHost(str(global_data.ftp_host), str(global_data.ftp_user), str(global_data.ftp_password)) as host:
        with host.open(file, 'rb') as f:
            table_name = create_table(file,conn)
            file_name =  file.split('/')[-1]

            download_file = os.path.dirname(os.path.abspath(__file__)) + '/download_file/' + file_name
            if os.path.isfile(download_file):
                os.remove(download_file)
                logger.info(f'Существующий файл {file_name} удален')

            logger.info(f'Скачивание файла: {file_name}')
            file = host.download(str(file), str(download_file))
            try:
                new_encoding = 'cp866'
                if encoding == "windows-1251":
                    new_encoding = "cp1251"
                elif encoding == "windows-1253":
                    new_encoding = "cp866"
                elif encoding == "IBM866":
                    new_encoding = "cp866"
                elif encoding == "MacRoman":
                    new_encoding = "MacRoman"
                elif encoding.lower() == "ascii":
                    new_encoding = "ascii"
                table = DBF(download_file,recfactory=UserDict , load=True, encoding=str(new_encoding))
                global_data.all_rows += len(table)
            except Exception as exc:
                logger.error(f'Не удалось открыть файл DBF: {file}. Ошибка: {exc}')
                return
            first_field = ''
            fields = table.field_names
            logger.info('Выполняется запись в БД')
            for record in tqdm(table, desc='Выполняется запись в БД'):
                data_dict = {}
                for i, f in enumerate(fields):
                    if first_field == '':
                        first_field = f
                    data_dict[f] = str(record.get(f))
                try:
                    cursor = conn.cursor()
                    insert_query = f'''insert into {table_name} (blob) values (%s)'''
                    data = json.dumps(data_dict)
                    cursor.execute(insert_query, (data,))
                    conn.commit()
                    cursor.close()
                    global_data.accept += 1
                except (Exception, psycopg2.DatabaseError) as error:
                    logger.error(error)
                    global_data.mass.append(i)
                    global_data.count_error += 1
                    conn.rollback()
                    cursor.close()
                    return
            try:
                os.remove(download_file)
                logger.info(f'Файл {file_name} успешно удален')
            except Exception as exc:
                logger.error(f'Не удалось удалить файл: {download_file}')
            logger.info(f'{table_name}:{len(table.records)} записей. Успешных:{global_data.accept} записей. Ошибок:{global_data.count_error}')
            global_data.accept = 0
            global_data.count_error = 0
            return True

