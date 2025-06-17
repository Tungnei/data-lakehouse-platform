# ==================================================================================
# SCRIPT: Validate Silver Layer Data Quality using PySpark and Deequ
# ==================================================================================
# Purpose:
#   - Perform data quality checks (completeness, minimum value, uniqueness) on Silver layer tables.
#   - Use PyDeequ to define and execute validation rules on key business entities.
#   - Log validation results for monitoring and debugging.
# ==================================================================================

import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pydeequ.checks import *
from pydeequ.verification import *

BASE_DIR = Path(__file__).resolve().parent.parent.parent

LOG_FILE = BASE_DIR / "logger" / "data_validation.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Silver Layer Data Quality Check") \
    .config("spark.sql.shuffle.partitions", "9") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.warehouse.dir", str(BASE_DIR / "tmp" / "sql_warehouse")) \
    .config("spark.local.dir", str(BASE_DIR / "tmp" / "local_dir")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ================== DEFINE HELPERS ==================
def read_parquet_cached(path: str):
    return spark.read.parquet(path).cache()

def run_check(df, check: Check, label: str):
    result = VerificationSuite(spark).onData(df).addCheck(check).run()
    logger.info(f"[{label}] - Data Quality Check Completed")
    return result

def check_uniqueness(df, column_name: str, label: str):
    total = df.count()
    distinct = df.select(countDistinct(column_name)).first()[0]
    if total == distinct:
        logger.info(f"[{label}] - Data Constraint Check Passed")
    else:
        logger.info(f"[{label}] - Data Constraint Check Failed")

# LOAD DATA
silver_path = "s3a://silver-layer"

product_df = read_parquet_cached(f"{silver_path}/slv.products")
store_df = read_parquet_cached(f"{silver_path}/slv.stores")
payment_method_df = read_parquet_cached(f"{silver_path}/slv.payment_method")
order_df = read_parquet_cached(f"{silver_path}/slv.orders")
order_detail_df = read_parquet_cached(f"{silver_path}/slv.order_details")

# Products
check_uniqueness(product_df, "product_id", "slv.products")

check_product = Check(spark, CheckLevel.Error, "slv.products") \
    .hasCompleteness("product_id", lambda x: x >= 1.0) \
    .hasCompleteness("product_name", lambda x: x >= 1.0) \
    .hasMin("unit_price", lambda x: x >= 0) \
    .hasCompleteness("category", lambda x: x >= 1.0) \
    .hasCompleteness("updated_at", lambda x: x >= 1.0)

run_check(product_df, check_product, "slv.products")

# Stores
check_uniqueness(store_df, "store_id", "slv.stores")

check_store = Check(spark, CheckLevel.Error, "slv.stores") \
    .hasCompleteness("store_id", lambda x: x >= 1.0) \
    .hasCompleteness("address", lambda x: x >= 1.0) \
    .hasCompleteness("district", lambda x: x >= 1.0) \
    .hasCompleteness("city", lambda x: x >= 1.0) \
    .hasCompleteness("updated_at", lambda x: x >= 1.0)

run_check(store_df, check_store, "slv.stores")

# Payment Method
check_uniqueness(payment_method_df, "method_id", "slv.payment_method")

check_payment_method = Check(spark, CheckLevel.Error, "slv.payment_method") \
    .hasCompleteness("method_id", lambda x: x >= 1.0) \
    .hasCompleteness("updated_at", lambda x: x >= 1.0)

run_check(payment_method_df, check_payment_method, "slv.payment_method")

# Orders
check_uniqueness(order_df, "order_id", "slv.orders")

check_order = Check(spark, CheckLevel.Error, "slv.orders") \
    .hasCompleteness("order_id", lambda x: x >= 1.0) \
    .hasCompleteness("store_id", lambda x: x >= 1.0) \
    .hasCompleteness("customer_id", lambda x: x >= 1.0) \
    .hasCompleteness("payment_method_id", lambda x: x >= 1.0)

run_check(order_df, check_order, "slv.orders")

# Order Details
check_order_detail = Check(spark, CheckLevel.Error, "slv.order_details") \
    .hasCompleteness("order_id", lambda x: x >= 1.0) \
    .hasCompleteness("product_id", lambda x: x >= 1.0) \
    .hasCompleteness("quantity", lambda x: x >= 1.0) \
    .hasCompleteness("subtotal", lambda x: x >= 1.0) \
    .hasMin("subtotal", lambda x: x >= 0)

run_check(order_detail_df, check_order_detail, "slv.order_details")

spark.stop()
os._exit(0)
