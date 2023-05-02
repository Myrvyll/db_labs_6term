import psycopg2
import logging
import re
import pandas as pd
import time
import numpy
import os

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s - %(message)s')

USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
DATABASE = os.getenv('POSTGRES_DB')
PORT = 5432
RESULTS_FILENAME = os.getenv('DATABASE_OUTPUT_FILENAME')

logger.info(f"{USER}, {PASSWORD}, {DATABASE}, {PORT}, {RESULTS_FILENAME}")


def get_headers(file_info: dict):
    with open(file_info['filename'], encoding=file_info['encoding']) as f:
        headers = f.readline()
        headers = re.findall('[\w\d]+', headers.lower())
    return headers


def union_lists(list1, list2):
    result = list1.copy()
    addition = list(set(list2)- set(list1))
    addition.sort()
    result.extend(addition)
    return result


def get_columns_descriptions(headers, primary_key=None, not_nulls=[], default_type='varchar', **types):
    headers_described = []
    dictionary = {}
    not_nulls = set(not_nulls)
    for item in types.values():
        item = set(item)

    for header in headers:
        dictionary['header'] = header
        dictionary['type'] = default_type
        if header == primary_key:
            dictionary['nullability'] = 'PRIMARY KEY'
        elif header in not_nulls:
            dictionary['nullability'] = 'NOT NULL'
        else:
            dictionary['nullability'] = 'NULL'

        for key, values in types.items():
            if header in values:
                dictionary['type'] = str(key)
        headers_described.append(dictionary.copy())

    return headers_described


def generate_sql_create_table(name: str, description: dict):
    line_opener = f'CREATE TABLE IF NOT EXISTS {name} ('
    line_ending = ');'
    filling = [' '.join(list(x.values())) for x in description]
    filling = ', '.join(filling)

    return line_opener + filling + line_ending
 


def nan_to_null(f,
        _NULL=psycopg2.extensions.AsIs('NULL'),
        _Float=psycopg2.extensions.Float):
    if not pd.isna(f):
        return _Float(f)
    return _NULL

psycopg2.extensions.register_adapter(float, nan_to_null)


def convert(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))


def write_file_to_db(file_info, chunksize):
    # get connection to database
    connection = None
    cursor = None
    marker = None
    year = int(file_info['file_id'].replace('ZNO', ''))
    columns_count = len(file_info['headers']) + 1
    names = file_info['headers'].copy()
    names.append('testyear')
    
    headers_string = ', '.join(names)
    headers_string = '(' + headers_string +')'
    
    while True:
        try:
            break
        except psycopg2.Error as error:
            logger.warning(error)
            time.sleep(15)
    
    # create queries layout
    query_insert = 'INSERT INTO ZNOData ' + headers_string + ' VALUES'
    query_log = 'INSERT INTO TransactionLog (transaction_file, transaction_time, transaction_volume) VALUES '
    s_line = "(" + "%s,"*(columns_count - 1) + "%s)"
    
    # write  to database
    while True:
        try:
            connection = psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, port=PORT, host='postgres_db')
            cursor = connection.cursor()
            cursor.execute("SELECT sum(transaction_volume) FROM TransactionLog WHERE transaction_file = %s", (file_info['file_id'], ))
            marker = cursor.fetchone()[0]
            connection.commit()
            if marker is None:
                marker = 0
            logger.debug(f'Starting from line: {marker}')

            f19 = pd.read_csv(file_info['filename'], sep=';', header=0, names=file_info['headers'], \
                              encoding=file_info['encoding'], chunksize=chunksize, skiprows=marker, decimal=',',\
                              engine='c')
            

            for k, chunk in enumerate(f19):
                # print(f'Its {k+1 + marker//chunksize} block!')
                args_list = ','.join(cursor.mogrify(s_line, numpy.append(x, year)).decode('utf-8') for x in chunk.values)
                cursor.execute(query_insert + args_list)
                log_query = query_log + cursor.mogrify('(%s, NOW(), %s)', [file_info['file_id'], len(chunk)]).decode('utf-8')
                cursor.execute(log_query)
                connection.commit()
                logger.info(f"{file_info['file_id']}: {k+1 + marker//chunksize} chunk has been written.")
            
            logger.info(f'File {file_info["file_id"]} was loaded into database.\n')
            break

        except psycopg2.Error as error:
            logger.warning(error)
            time.sleep(15)
        except FileNotFoundError as error:
            logger.error(error)
    cursor.close()
    connection.close()
    



FILE_19_INFO = {'filename': r'../data/Odata2019File.csv',
                'delimeter': ';',
                'encoding': 'cp1251',
                'file_id': 'ZNO2019'}

FILE_21_INFO = {'filename': r'../data/Odata2021File.csv',
                'delimeter': ';',
                'encoding': 'utf-8',
                'file_id': 'ZNO2021'}


time_start = time.time()
FILE_19_INFO['headers'] = get_headers(FILE_19_INFO)
FILE_21_INFO['headers'] = get_headers(FILE_21_INFO)

common_headers = union_lists(FILE_19_INFO['headers'], FILE_21_INFO['headers'])
common_headers.insert(2, 'testyear')
float_columns = list(filter(lambda x: re.match('[\w]+ball100', x), common_headers))

int_columns = list(filter(lambda x: re.match('([\w]+ball12$)|([\w]+ball$)|([\w]+adaptscale)', x), common_headers))
int_columns.extend(['birth', 'testyear'])


not_nullable = common_headers[:9]

columns_descriptions = get_columns_descriptions(common_headers, primary_key='outid', not_nulls=not_nullable, bigint=int_columns, real=float_columns)

sql_command_create_main_table = generate_sql_create_table('ZNOData', columns_descriptions)
logger.info('SQL request generated.')
sql_command_create_log_table = \
'''CREATE TABLE IF NOT EXISTS TransactionLog (
       transaction_id      SERIAL   PRIMARY KEY,
       transaction_file    varchar  NOT NULL,
       transaction_time    time     NOT NULL,
       transaction_volume  int      NULL);'''

# create database
for _ in range(5):
    try:
        connection = psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, port=PORT, host='postgres_db')
        cursor = connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS znodata CASCADE')
        cursor.execute('DROP TABLE IF EXISTS transactionlog CASCADE')
        cursor.execute(sql_command_create_main_table)
        cursor.execute(sql_command_create_log_table)
        connection.commit()
        connection.close()
        break
    except psycopg2.Error as error:
        logger.error(error)
        time.sleep(15)


    
FILE_19_INFO['headers'] = [re.sub('^ukr', 'uml', x) for x in FILE_19_INFO['headers']]

write_file_to_db(FILE_19_INFO, chunksize=2000)
time_end1 = time.time()
logger.info(f'Time for 1 file: {convert(time_end1 - time_start)}')

write_file_to_db(FILE_21_INFO, chunksize=2000)
time_end2 = time.time()
logger.info(f'Time for 2 file: {convert(time_end2 - time_end1)}')
logger.info(f'Total time: {convert(time_end2 - time_start)}')

with open('time.txt', mode='w') as t:
    t.write(f'Time for 1 file: {convert(time_end1 - time_start)}\n')
    t.write(f'Time for 2 file: {convert(time_end2 - time_end1)}\n')
    t.write(f'Total time: {convert(time_end2 - time_start)}\n')


query_summary = '''
SELECT regname, 
       max(umlball100) FILTER (WHERE testyear=2019) AS max2019,
	   max(umlball100) FILTER (WHERE testyear=2021) AS max2021
FROM znodata
WHERE umlteststatus = 'Зараховано'
GROUP BY regname'''

result = None
for _ in range(3):
    try:
        connection = psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, port=PORT, host='postgres_db')
        cursor = connection.cursor()
        cursor.execute(query_summary)
        result = cursor.fetchall()
        connection.commit()
        connection.close()
        break
    except psycopg2.Error as error:
        logger.error(error)
        time.sleep(15)

result = pd.DataFrame(result, columns=['region_name', 'uml_max_ball_2019', 'uml_max_ball_2021'])
result.to_csv(RESULTS_FILENAME, index=False)
logger.info(f'Query results were written to "{RESULTS_FILENAME}".')