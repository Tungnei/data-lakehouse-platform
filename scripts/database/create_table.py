# ==================================================================================
# SCRIPT: Create MySQL Source Tables for Data Pipeline
# ==================================================================================
# Purpose:
#   - Define and create raw source tables in a MySQL database.
#   - These tables serve as the initial input layer for the data pipeline.
#       → orders
#       → order_details
#       → customers
#       → products
#       → stores
#       → payment_methods
#   - These tables will be populated with sample or production data.
# ==================================================================================
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

import mysql.connector
from mysql.connector import errorcode

BASE_DIR = Path(__file__).resolve().parent.parent.parent

TABLES = {}
TABLES['stores'] = (
    "CREATE TABLE `stores` (" \
    "   `id` int," \
    "   `name` varchar(20) NOT NULL,"
    "   `address` varchar(250) NOT NULL," \
    "   `district` varchar(50) NOT NULL," \
    "   `city` varchar(50) NOT NULL," \
    "   `updated_at` DATETIME," \
    "   PRIMARY KEY (`id`)" \
    ") ENGINE=InnoDB"
)

TABLES['payment_method'] = (
    "CREATE TABLE `payment_method` (" \
    "   `id` int," \
    "   `method_name` varchar(25) NOT NULL," \
    "   `bank` varchar(25) NOT NULL," \
    "   `updated_at` DATETIME," \
    "   PRIMARY KEY (`id`)" \
    ") ENGINE=InnoDB"
)

TABLES['product_category'] = (
    "CREATE TABLE `product_category` (" \
    "   `id` int," \
    "   `name` varchar(25) NOT NULL," \
    "   `updated_at` DATETIME," \
    "   PRIMARY KEY (`id`)" \
    ") ENGINE=InnoDB"
)

TABLES['products'] = (
    "CREATE TABLE `products` (" \
    "   `id` varchar(25) NOT NULL," \
    "   `name` varchar(50) NOT NULL," \
    "   `category_id` int NOT NULL," \
    "   `unit_price` int NOT NULL," \
    "   `updated_at` DATETIME," \
    "   PRIMARY KEY (`id`)," \
    "   CONSTRAINT `products_fk_1` FOREIGN KEY (`category_id`)" \
    "       REFERENCES `product_category` (`id`) ON DELETE CASCADE" \
    ") ENGINE=InnoDB"
)

TABLES['customers'] = (
    "CREATE TABLE `customers` (" \
    "   `id` int NOT NULL," \
    "   `name` varchar(25) NOT NULL," \
    "   `phone_number` int NOT NULL," \
    "   `tier` ENUM('regular', 'silver', 'gold', 'diamond') NOT NULL," \
    "   `updated_at` DATETIME," \
    "   PRIMARY KEY (`id`)" \
    ") ENGINE=InnoDB"
)

TABLES['orders'] = (
    "CREATE TABLE `orders` (" \
    "   `id` varchar(250) NOT NULL," \
    "   `timestamp` datetime NOT NULL," \
    "   `customer_id` int," \
    "   `store_id` int NOT NULL," \
    "   `payment_method_id` int NOT NULL," \
    "   `num_products` int NOT NULL," \
    "   PRIMARY KEY (`id`)," \
    "   CONSTRAINT `orders_fk_1` FOREIGN KEY (`store_id`)" \
    "       REFERENCES `stores` (`id`)," \
    "   CONSTRAINT `orders_fk_2` FOREIGN KEY (`payment_method_id`)" \
    "       REFERENCES `payment_method` (`id`) ON DELETE CASCADE" \
    ") ENGINE=InnoDB"
)

TABLES['order_details'] = (
    "CREATE TABLE `order_details` (" \
    "   `order_id` varchar(250) NOT NULL," \
    "   `product_id` varchar(25) NOT NULL," \
    "   `quantity` int NOT NULL," \
    "   `discount_percent` int NOT NULL DEFAULT 0," \
    "   `subtotal` int NOT NULL," \
    "   `is_suggestion` BOOLEAN NOT NULL DEFAULT FALSE," \
    "   CONSTRAINT `order_details_fk_1` FOREIGN KEY (`order_id`)" \
    "       REFERENCES `orders` (`id`) ON DELETE CASCADE," \
    "   CONSTRAINT `order_details_fk_2` FOREIGN KEY (`product_id`)" \
    "       REFERENCES `products` (`id`) ON DELETE CASCADE" \
    ") ENGINE=InnoDB"
)

def connect_database(user, password, host, database):
    """Connect to MySQL database."""
    try:
        print(f"Running script: {__file__}")

        conn = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database=database
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username and password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        sys.exit(1)
    else:
        print("Connect to database successfully!")
        return conn

def create_table(cursor):
    """Create tables in MySQl database."""
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print(f"Creating table {table_name}: ", end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
    

if __name__ == "__main__":
    # Read .env path
    dotenv_path = BASE_DIR / '.env'
    load_dotenv(dotenv_path)

    # Configuration to connect to MySQL database
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST")
    database = os.getenv("MYSQL_DATABASE")

    conn = connect_database(user=user, password=password, host=host, database=database)
    cursor = conn.cursor()
    try:
        create_table(cursor)
    finally:
        conn.close()
        cursor.close()