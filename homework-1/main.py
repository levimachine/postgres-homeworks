import csv
import psycopg2

try:
    with psycopg2.connect(host='localhost', database='north', user='postgres', password='12345') as conn:
        with conn.cursor() as cur:
            with open('north_data/customers_data.csv', 'r', encoding='utf-8') as file:
                csv_dict = csv.DictReader(file)
                for row in csv_dict:
                    cur.execute("INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                                (row['customer_id'], row['company_name'], row['contact_name']))
            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as file:
                csv_dict = csv.DictReader(file)
                for row in csv_dict:
                    cur.execute(
                        "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s,%s, %s, %s)",
                        (row['employee_id'], row['first_name'], row['last_name'], row['title'], row['birth_date'],
                         row['notes']))
            with open('north_data/orders_data.csv', 'r', encoding='utf-8') as file:
                csv_dict = csv.DictReader(file)
                for row in csv_dict:
                    cur.execute(
                        "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s,%s, %s)",
                        (row['order_id'], row['customer_id'], row['employee_id'], row['order_date'], row['ship_city']))
finally:
    conn.close()
