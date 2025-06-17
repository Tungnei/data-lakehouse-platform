# ==================================================================================
# SCRIPT: Validate Bronze Layer Data Quality and Schema using PySpark
# ==================================================================================
# Purpose:
#   - Perform schema validation against stored schema definitions for Bronze tables.
#   - Check data quality rules: nullability and uniqueness constraints per table.
#   - Log issues and updates for monitoring and debugging.
# ==================================================================================

import random
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import mysql.connector
import redis
from faker import Faker
from mysql_config import get_mysql_config

fake = Faker()

BASE_DIR = Path(__file__).resolve().parent.parent.parent

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


def create_order(cursor, order_id, customer_id, store_id, payment_method_id, timestamp):
    cursor.execute(
        """
        INSERT INTO orders (id, timestamp, customer_id, store_id, payment_method_id, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (order_id, timestamp, customer_id, store_id, payment_method_id, "pending")
    )


def insert_order_items(cursor, order_id, products):
    num_items = random.choices([1, 2, 3], weights=[0.75, 0.15, 0.1])[0]
    selected = random.sample(products, num_items)

    for product in selected:
        quantity = random.choices([1, 2, 3], weights=[0.875, 0.1, 0.025])[0]
        subtotal = product["unit_price"] * quantity
        cursor.execute(
            """
            INSERT INTO order_details (order_id, product_id, quantity, subtotal)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product["id"], quantity, subtotal)
        )
        time.sleep(0.05)

    return num_items


def save_to_redis(order_id, customer_id, payment_method_id, num_items):
    r = redis.Redis(host="localhost", port=6379, db=1)
    r.hset(
        f"order:{order_id}",
        mapping={
            "customer_id": customer_id,
            "payment_method_id": payment_method_id,
            "status": 'pending',
            "num_products": num_items 
        }
    )
    r.expire(f"order:{order_id}", 100) 


# def main():
#     with get_conn_cursor() as (conn, cursor):
#         products = get_products(cursor)

#         while True:
#             order_id = fake.uuid4()
#             timestamp = datetime.now()
#             customer_id = random.randint(1, 1000000)
#             store_id = random.randint(1, 1000)
#             payment_method_id = random.randint(1, 12)

#             try:
#                 create_order(cursor, order_id, customer_id, store_id, payment_method_id, timestamp)
#                 num_items = insert_order_items(cursor, order_id, products)
#                 save_to_redis(order_id, customer_id, payment_method_id, num_items)
#                 conn.commit()
#                 print(f"Created order {order_id} with {num_items} item(s)")
#             except Exception as insert_err:
#                 conn.rollback()
#                 print(f"Failed to insert order {order_id}: {insert_err}")

#             time.sleep(0.01)

def main():
    with get_conn_cursor() as (conn, cursor):
        products = get_products(cursor)

        minute_orders = 0
        start_time = time.time()
        last_report_time = start_time

        while True:
            order_id = fake.uuid4()
            timestamp = datetime.now()
            customer_id = random.randint(1, 20000)
            store_id = random.randint(1, 1000)
            payment_method_id = random.randint(1, 9)

            try:
                create_order(cursor, order_id, customer_id, store_id, payment_method_id, timestamp)
                num_items = insert_order_items(cursor, order_id, products)
                save_to_redis(order_id, customer_id, payment_method_id, num_items)
                conn.commit()
                minute_orders += 1
                print(f"✅ Created order {order_id} with {num_items} product(s)")
            except Exception as insert_err:
                conn.rollback()
                print(f"❌ Failed to insert order {order_id}: {insert_err}")

            # Kiểm tra nếu đã đủ 60 giây để in báo cáo
            now = time.time()
            if now - last_report_time >= 60:
                print("=" * 80)
                print(f"⏱ 1 phút vừa qua: {minute_orders} orders")
                print("=" * 80)
                minute_orders = 0
                last_report_time = now

            time.sleep(0.0001)


if __name__ == "__main__":
    main()