import mysql.connector


# class NormalizeTime():
def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


@singleton
class Database(object):
    # mysql database credentials
    config = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "zaman",
        "password": "Zaman@95",
        "database": "scrapped_courses_data",
        "use_unicode": True,
        "raise_on_warnings": True
    }
    def __init__(self):
        try:
            conn = mysql.connector.connect(**self.config)
        except mysql.connector.Error as e:
            logger(e, level=40)
            sys.exit('Could not connect to database')

        self.connection = conn

    def connect(self):
        return self.connection


def update_time(table_name):
    database = Database()
    connection = database.connect()
    cursor = connection.cursor()

    # get time_to_complete column values by vendor
    vendor = 'Udemy'
    sql = f"SELECT product_id, time_to_complete, vendor, name from {table_name}"
    cursor.execute(sql)
    products = cursor.fetchall()
    length = len(products)
    count = 0

    for product in products:
        converted_time = 0
        try:
            if not product[1]:
                converted_time = 0

            elif product[2] == 'Coursera': # "47 hours"
                converted_time = int(product[1].split(' ')[0]) * 3600

            elif product[2] == 'Datacamp': # "4 Hours"
                converted_time = int(product[1].split(' ')[0]) * 3600

            elif product[2] == 'Edureka': # null
                converted_time = 0

            elif product[2] == 'EDX': # "8 week to complete"
                converted_time = int(product[1].split(' ')[0]) * 4 * 3600 # 1 week = 4 hour

            elif product[2] == 'Excelsior': # "8 week"
                converted_time = int(product[1].split(' ')[0]) * 4 * 3600 # 1 week = 4 hour

            elif product[2] == 'Futurelearn': # "6 weeks"
                converted_time = int(product[1].split(' ')[0]) * 4 * 3600 # 1 week = 4 hour

            elif product[2] == 'Linkedin': # "2h 55m" "37m" "2h"
                time = product[1].split(' ')
                for data in time:
                    ext = data[-1]
                    data = data.replace(ext, '')
                    if ext == 'd':
                        converted_time += int(data) * 24 * 3600
                    elif ext == 'h':
                        converted_time += int(data) * 3600
                    elif ext == 'm':
                        converted_time += int(data) * 60

            elif product[2] == 'Skillshare': # "1d 3h 33m" "6h 45m" "16m"
                time = product[1].split(' ')
                for data in time:
                    ext = data[-1]
                    data = data.replace(ext, '')
                    if ext == 'd':
                        converted_time += int(data) * 24 * 3600
                    elif ext == 'h':
                        converted_time += int(data) * 3600
                    elif ext == 'm':
                        converted_time += int(data) * 60

            elif product[2] == 'Stanford Online': # "10-15 hours per week" "9 hours" "20-22 Hours" chooses the highest value to convert
                time = product[1].split(' ')[0]
                time = time.split('-')
                t_len = len(time)
                converted_time = int(time[t_len - 1]) * 3600

            elif product[2] == 'Udemy': # "06:35:52" "31:35"
                time = product[1].split(':')
                if len(time) == 3:
                    converted_time = int(time[0]) * 3600 + int(time[1]) * 60 + int(time[2])
                elif len(time) == 2:
                    converted_time = int(time[0]) * 60 + int(time[1])
                elif len(time) == 1:
                    converted_time = int(time[0])

        except Exception as e:
            print(product)
            print(e)

        count += 1
        print("---------------------------------------------")
        print(count, converted_time)

        sql = f"UPDATE {table_name} SET time_to_complete = %s WHERE product_id = {product[0]}"
        values = tuple([converted_time])

        try:
            cursor.execute(sql, values)
            row_id = cursor.lastrowid
            connection.commit()
            count += 1
            print(f'updated {count}/{length}, row_id = {row_id}, product_id = {product[0]}')
        except mysql.connector.Error as e:
            print(f'Could not update row {product[0]} in table {table_name}')
            print(e)
    return True


if __name__ == '__main__':
    print('#################### SUMMARY ####################')
    table_name = 'products'
    update_time(table_name)