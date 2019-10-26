import multiprocessing
import mysql.connector
import argparse
import datetime
import asyncio
import string
import random
import signal
import time
import math
import sys
import re

db_host = ''
db_port = ''
db_user = ''
db_passwd = ''
db_name = ''


def open_db_connect():
    return mysql.connector.connect(host=db_host, port=db_port, user=db_user, passwd=db_passwd, database=db_name)


class ColumnDefine:
    def __init__(self, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, column_type):
        self.column_name = column_name
        self.data_type = data_type
        self.character_maximum_length = character_maximum_length
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale
        self.datetime_precision = datetime_precision
        self.column_type = column_type


def check_unsigned(column_define):
    return column_define.column_type.endswith('unsigned')


def generate_bit_value(column_define):
    min_value = 0
    max_value = math.pow(2, column_define.numeric_precision)
    return random.randint(min_value, max_value)


def generate_tinyint_value(column_define):
    if check_unsigned(column_define):
        min_value = 0
        max_value = 255
    else:
        min_value = -128
        max_value = 127
    return random.randint(min_value, max_value)


def generate_smallint_value(column_define):
    if check_unsigned(column_define):
        min_value = 0
        max_value = 65535
    else:
        min_value = -32768
        max_value = 32767
    return random.randint(min_value, max_value)


def generate_mediumint_value(column_define):
    if check_unsigned(column_define):
        min_value = 0
        max_value = 16777215
    else:
        min_value = -8388608
        max_value = 8388607
    return random.randint(min_value, max_value)


def generate_int_value(column_define):
    if check_unsigned(column_define):
        min_value = 0
        max_value = 4294967295
    else:
        min_value = -2147483648
        max_value = 2147483647
    return random.randint(min_value, max_value)


def generate_bigint_value(column_define):
    if check_unsigned(column_define):
        min_value = 0
        max_value = 18446744073709551615
    else:
        min_value = -9223372036854775808
        max_value = 9223372036854775807
    return random.randint(min_value, max_value)


def random_fixed_length_int(length):
    return ''.join(random.choices(string.digits, k=length))


def random_decimal(integer, deciaml):
    return random_fixed_length_int(integer).strip('0') + '.' + random_fixed_length_int(deciaml)


def generate_decimal_value(column_define):
    if check_unsigned(column_define):
        min_value = 0
        max_value = 18446744073709551615
    else:
        min_value = -9223372036854775808
        max_value = 9223372036854775807
    return random.randint(min_value, max_value)


def generate_decimal_value(column_define):
    integer = column_define.numeric_precision - column_define.numeric_scale
    deciaml = column_define.numeric_scale
    if check_unsigned(column_define):
        return random_decimal(integer, deciaml)
    else:
        return (''.join(random.choices(['-', '+'], k=1)) + random_decimal(integer, deciaml)).strip('+')


def generate_float_value(column_define):
    column_define.numeric_scale = 5
    return generate_decimal_value(column_define)


def generate_double_value(column_define):
    column_define.numeric_scale = 10
    return generate_decimal_value(column_define)


def generate_date_value(column_define):
    return str(random.randint(1000, 9999)) + '-' + str(random.randint(1, 12)) + '-' + str(random.randint(1, 28))


def generate_fraction_value(column_define):
    if 0 < column_define.datetime_precision:
        return '.' + random_fixed_length_int(column_define.datetime_precision)
    else:
        return ''


def generate_datetime_value(column_define):
    return generate_date_value(column_define) + ' ' + str(random.randint(0, 23)) + ':' + str(random.randint(0, 59)) + ':' + str(random.randint(0, 59)) + generate_fraction_value(column_define)


def generate_timestamp_value(column_define):
    return datetime.datetime.fromtimestamp(random.randint(1, 2147454847)).strftime('%Y-%m-%d %H:%M:%S') + generate_fraction_value(column_define)


def generate_time_value(column_define):
    return str(random.randint(-838, 838)) + ':' + str(random.randint(0, 59)) + ':' + str(random.randint(0, 59)) + generate_fraction_value(column_define)


def generate_year_value(column_define):
    return random.randint(1901, 2155)


def generate_char_value(column_define):
    max_length = random.randint(0, 100 if column_define.character_maximum_length >
                                100 else column_define.character_maximum_length)
    return ''.join(random.choices(string.printable, k=max_length))


def generate_varchar_value(column_define):
    return generate_char_value(column_define)


def generate_binary_value(column_define):
    return generate_char_value(column_define)


def generate_varbinary_value(column_define):
    return generate_char_value(column_define)


def generate_tinyblob_value(column_define):
    return generate_char_value(column_define)


def generate_tinytext_value(column_define):
    return generate_char_value(column_define)


def generate_blob_value(column_define):
    return generate_char_value(column_define)


def generate_text_value(column_define):
    return generate_char_value(column_define)


def generate_mediumblob_value(column_define):
    return generate_char_value(column_define)


def generate_mediumtext_value(column_define):
    return generate_char_value(column_define)


def generate_longblob_value(column_define):
    return generate_char_value(column_define)


def generate_longtext_value(column_define):
    return generate_char_value(column_define)


def generate_set_value(column_define):
    match = re.findall(r'\'([^\']*)\'', column_define.column_type)
    return match[random.randint(0, len(match) - 1)]


def generate_enum_value(column_define):
    return generate_set_value(column_define)


def generate_json_value(column_define):
    return '{}'


value_generator_map = {}
value_generator_map['bit'] = generate_bit_value
value_generator_map['tinyint'] = generate_tinyint_value
value_generator_map['smallint'] = generate_smallint_value
value_generator_map['mediumint'] = generate_mediumint_value
value_generator_map['int'] = generate_int_value
value_generator_map['bigint'] = generate_bigint_value
value_generator_map['decimal'] = generate_decimal_value
value_generator_map['float'] = generate_float_value
value_generator_map['double'] = generate_double_value
value_generator_map['date'] = generate_date_value
value_generator_map['datetime'] = generate_datetime_value
value_generator_map['timestamp'] = generate_timestamp_value
value_generator_map['time'] = generate_time_value
value_generator_map['year'] = generate_year_value
value_generator_map['char'] = generate_char_value
value_generator_map['varchar'] = generate_varchar_value
value_generator_map['binary'] = generate_binary_value
value_generator_map['varbinary'] = generate_varbinary_value
value_generator_map['tinyblob'] = generate_tinyblob_value
value_generator_map['tinytext'] = generate_tinytext_value
value_generator_map['blob'] = generate_blob_value
value_generator_map['text'] = generate_text_value
value_generator_map['mediumblob'] = generate_mediumblob_value
value_generator_map['mediumtext'] = generate_mediumtext_value
value_generator_map['longblob'] = generate_longblob_value
value_generator_map['longtext'] = generate_longtext_value
value_generator_map['set'] = generate_set_value
value_generator_map['enum'] = generate_enum_value
value_generator_map['json'] = generate_json_value


def generate_value(column_define):
    return value_generator_map[column_define.data_type](column_define)


def get_column_defines(db_name, table_name):
    result = []
    db = open_db_connect()
    cursor = db.cursor()
    cursor.execute("select column_name,data_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision,column_type from information_schema.columns where table_schema = '%s' and table_name = '%s'" % (db_name, table_name))
    for row in cursor.fetchall():
        result.append(ColumnDefine(
            row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
    db.close()
    return result


running = True


async def do_insert(table_name, column_defines):
    columns_str = ''
    values_str = ''
    for item in column_defines:
        if 'id' != item.column_name.lower():
            columns_str += '`' + item.column_name + '`,'
            values_str += '%s,'
    sql = 'insert into %s(%s) values(%s)' % (
        table_name, columns_str.rstrip(','), values_str.rstrip(','))

    db = open_db_connect()
    cursor = db.cursor()
    counter = 0
    while running:
        for i in range(random.randint(1, 200)):
            values = []
            for item in column_defines:
                if 'id' != item.column_name.lower():
                    values.append(generate_value(item))
            cursor.execute(sql, values)
            counter += 1
        db.commit()
        await asyncio.sleep(0.01)
    print('%s,insert count:%d' % (table_name, counter))
    db.close()


async def do_update(table_name, column_defines):
    global running
    db = open_db_connect()
    cursor = db.cursor()
    counter = 0
    while running:
        cursor.execute('select id from %s order by rand() limit %s' %
                       (table_name, random.randint(1, 20)))
        ids = cursor.fetchall()
        for i in ids:
            sql = 'update %s set ' % (table_name)
            updated = random.sample(
                column_defines, k=random.randint(2, len(column_defines)))
            values = []
            for item in updated:
                if 'id' != item.column_name.lower():
                    sql += '%s=%s,' % (item.column_name, '%s')
                    values.append(generate_value(item))
            sql = sql.rstrip(',') + ' where id = ' + str(i[0])
            cursor.execute(sql, values)
            counter += 1
        db.commit()
        await asyncio.sleep(0.01)
    print('%s,update count:%d' % (table_name, counter))
    db.close()


async def do_delete(table_name, column_defines):
    global running
    db = open_db_connect()
    cursor = db.cursor()
    counter = 0
    while running:
        cursor.execute('select id from %s order by rand() limit %s' %
                       (table_name, random.randint(1, 5)))
        ids = cursor.fetchall()
        for i in ids:
            sql = 'delete from %s where id = %s' % (table_name, i[0])
            cursor.execute(sql)
            counter += 1
        db.commit()
        await asyncio.sleep(0.01)
    print('%s,delete count:%d' % (table_name, counter))
    db.close()


async def do_stop(run_time):
    global running
    start_time = time.time()
    while running:
        if start_time + run_time <= time.time():
            running = False
            return
        else:
            await asyncio.sleep(0.01)


def exit(signum, frame):
    global running
    running = False


def run(table_names, duration):
    signal.signal(signal.SIGINT, exit)
    signal.signal(signal.SIGTERM, exit)

    tasks = []
    loop = asyncio.get_event_loop()
    for item in table_names:
        column_defines = get_column_defines(db_name, item)
        tasks.append(loop.create_task(do_insert(item, column_defines)))
        tasks.append(loop.create_task(do_update(item, column_defines)))
        tasks.append(loop.create_task(do_delete(item, column_defines)))
        tasks.append(loop.create_task(do_stop(duration)))
    loop.run_until_complete(asyncio.gather(*tasks))


def main():
    global db_host
    global db_port
    global db_user
    global db_passwd
    global db_name
    parser = argparse.ArgumentParser()
    parser.add_argument('db_host')
    parser.add_argument('db_port')
    parser.add_argument('db_user')
    parser.add_argument('db_passwd')
    parser.add_argument('db_name')
    parser.add_argument('table_names')
    parser.add_argument('duration')
    args = parser.parse_args()
    db_host = args.db_host
    db_port = args.db_port
    db_user = args.db_user
    db_passwd = args.db_passwd
    db_name = args.db_name
    table_names = args.table_names
    duration = args.duration
    run(table_names.split(','), int(duration))


main()
