# ==============================================
# SCRIPT: Generate data to MySQL database
# ==============================================
# Purpose:
#   - Simulate real-time coffee orders
# ==============================================

import random
import sys
import time
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

import mysql.connector
from faker import Faker

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from scripts.utils import get_mysql_config

fake = Faker()

# Configuration to connect to MySQL database
MYSQL_CONFIG = get_mysql_config()



@contextmanager
def get_conn_cursor():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()


def get_products(cursor):
    cursor.execute("SELECT id, name, unit_price FROM products")
    return cursor.fetchall()


def create_order(cursor, order_id, timestamp, customer_id,  
                 store_id, payment_method_id, num_products):
    
    cursor.execute(
        """
        INSERT INTO orders (id, timestamp, customer_id, store_id, payment_method_id, num_products)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (order_id, timestamp, customer_id, store_id, payment_method_id, num_products)
    )


def prepare_order_items(products):
    num_items = random.choices([1, 2, 3], weights=[0.75, 0.15, 0.1])[0]
    selected = random.sample(products, num_items)
    order_items = []

    for product in selected:
        quantity = random.choices([1, 2, 3], weights=[0.875, 0.1, 0.025])[0]
        subtotal = product["unit_price"] * quantity
        order_items.append((product["id"], quantity, subtotal))

    return order_items


def main():
    with get_conn_cursor() as (conn, cursor):
        products = get_products(cursor)

        while True:
            order_id = fake.uuid4()
            timestamp = datetime.now()
            customer_id = random.randint(1, 1000000)
            store_id = random.randint(1, 1000)
            payment_method_id = random.randint(1, 12)
            order_items = prepare_order_items(products)
            num_products = len(order_items)

            try:
                create_order(cursor, order_id, timestamp, customer_id, store_id, payment_method_id, num_products)
                for product_id, quantity, subtotal in order_items:
                        cursor.execute(
                            "INSERT INTO order_details (order_id, product_id, quantity, subtotal, is_suggestion) VALUES (%s, %s, %s, %s, %s)",
                            (order_id, product_id, quantity, subtotal, False)
                        )
                conn.commit()
                print(f"Created order {order_id} with {num_products} item(s)")
            except Exception as insert_err:
                conn.rollback()
                print(f"Failed to insert order {order_id}: {insert_err}")

            time.sleep(0.0001)


if __name__ == "__main__":
    main()
