import time
import logging
from pathlib import Path
from contextlib import contextmanager

import redis
import mysql.connector
from mysql_config import get_mysql_config

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Load configuration file
MYSQL_CONFIG = get_mysql_config()

@contextmanager
def get_cursor():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()

# Main logic to sync order status from Redis to MySQL
def main():
    with get_cursor() as (conn, cursor):
        completed_order_ids = []

        # Scan Redis keys and collect order_ids with 'completed' status
        for key in r.scan_iter("order_status:*"):
            order_id = key.split(":")[1]
            status = r.get(key)

            if status == "completed":
                completed_order_ids.append(order_id)

        if completed_order_ids:
            try:
                # Perform a single batch update for all completed orders
                format_strings = ','.join(['%s'] * len(completed_order_ids))
                update_query = f"UPDATE orders SET status = 'completed' WHERE id IN ({format_strings})"
                cursor.execute(update_query, completed_order_ids)
                conn.commit()

                logging.info(f"Updated {len(completed_order_ids)} orders to status 'completed'.")

                # Delete Redis keys after successful update
                for order_id in completed_order_ids:
                    r.delete(f"order_status:{order_id}")

            except Exception as e:
                conn.rollback()
                logging.error(f"Batch update failed: {e}")


if __name__ == "__main__":
    while True:
        logging.info("--- Starting update order status in MySQL ---")
        try:
            main()
        except Exception as e:
            logging.error(f"Exception in batch run: {e}")
        logging.info(f"--- Done update order status in MySQL ---")
        time.sleep(10 * 60)