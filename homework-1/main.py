"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

pass_db = open('C:/Users/MSI6e4ka/postgres-homeworks/pass_db.txt').read().strip()

conn = psycopg2.connect(database='north', user='postgres', password=pass_db)


def filling_db(file_name, name_table):
    with open(file_name, encoding='utf-8') as file:
        read_data = csv.reader(file)
        header = next(read_data)
        data_list = []
        for row in read_data:
            data_list.append(row)

        with conn:
            with conn.cursor() as cur:
                columns = ', '.join(header)
                placeholders = ', '.join(['%s'] * len(header))
                sql = f'INSERT INTO {name_table} ({columns}) VALUES ({placeholders})'
                cur.executemany(sql, data_list)


if __name__ == '__main__':
    filling_db('north_data/customers_data.csv', 'customers')
    filling_db('north_data/employees_data.csv', 'employees')
    filling_db('north_data/orders_data.csv', 'orders')
