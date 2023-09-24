"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

pass_db = open('C:/Users/MSI6e4ka/postgres-homeworks/pass_db.txt').read().strip()

customers_file = 'north_data/customers_data.csv'
employees_file = 'north_data/employees_data.csv'
orders_file = 'north_data/orders_data.csv'

conn = psycopg2.connect(database='north', user='postgres', password=pass_db)

with open(customers_file, encoding='utf-8') as file:
    read_data = csv.reader(file)
    next(read_data)
    data_list = []
    for row in read_data:
        data_list.append(row)

    with conn:
        with conn.cursor() as cur:
            for row in data_list:
                cur.execute('INSERT INTO customers (customer_id, company_name, contact_name) '
                            'VALUES (%s, %s, %s)', row)

with open(employees_file, encoding='utf-8') as file:
    read_data = csv.reader(file)
    next(read_data)
    data_list = []
    for row in read_data:
        data_list.append(row)

    with conn:
        with conn.cursor() as cur:
            for row in data_list:
                cur.execute('INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) '
                            'VALUES (%s, %s, %s, %s, %s, %s)', row)

with open(orders_file, encoding='utf-8') as file:
    read_data = csv.reader(file)
    next(read_data)
    data_list = []
    for row in read_data:
        data_list.append(row)

    with conn:
        with conn.cursor() as cur:
            for row in data_list:
                cur.execute('INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) '
                            'VALUES (%s, %s, %s, %s, %s)', row)

