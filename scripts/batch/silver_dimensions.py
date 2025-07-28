# =================================================================================
# SCRIPT: Transform and Load Dimension Tables to Silver Layer (s3a://silver-layer/)
# =================================================================================
# Purpose:
#   - Clean and transform raw data from the Bronze Layer (s3a://bronze-layer/)
#   - Write clean and structured dimension data to the Silver Layer in Parquet format
# =================================================================================

import sys
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

BASE_DIR = Path(__file__).resolve().parent.parent
# Setup logging
LOG_FILE = BASE_DIR / "logger" / "batch.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)

def create_SparkSession() -> SparkSession:
    return SparkSession.builder \
        .appName("Load Bronze to Silver Layer") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", str(BASE_DIR / "tmp" / "sql_warehouse")) \
        .config("spark.local.dir", str(BASE_DIR / "tmp" / "local_dir")) \
        .getOrCreate()
    

def read_bronze_layer(spark, table):
    return spark.read.parquet(f"s3a://bronze-layer/{table}")


def cleand_stores(spark, silver_path, table):
    """
    - The 'city' column in this table contains a '\r' character at the end of the string, 
      so we need to clean it.
    """
    logger.info(f"[SILVER][{table}] --- Starting transformation for 'stores' ---")
    source_df = read_bronze_layer(spark, table="brz.stores")
    cleaned_df = source_df.withColumn("city_cleaned", expr("regexp_replace(city, '\\\\r$', '')"))
    output_df = cleaned_df.selectExpr(
        "id AS store_id",
        "name AS store_name",
        "address",
        "district",
        "city_cleaned AS city",
        "updated_at"
    )

    output_df.write.mode("overwrite").parquet(f"{silver_path}/{table}")
    logger.info(f"[SILVER][{table}] Successfully wrote records to '{table}' table in Silver Layer.")


def cleand_products(spark, silver_path, table):
    """
    The 'products' and 'product_category' tables are already cleaned. 
    We just need to perform a join between them.
    """
    logger.info(f"[SILVER][{table}] --- Starting join and transformation for 'products' and 'product_category' ---")
    product = read_bronze_layer(spark, table="brz.products")
    product_category = read_bronze_layer(spark, table="brz.product_category")

    join_df = product.join(
        product_category,
        product["category_id"] == product_category["id"],
        how="left"
    ).select(
        product.id.alias("product_id"),
        product.name.alias("product_name"),
        product_category.name.alias("category"),
        product.unit_price,
        product.updated_at
    )

    join_df.write.mode("overwrite").parquet(f"{silver_path}/{table}")
    logger.info(f"[SILVER][{table}] Successfully wrote records to '{table}' table in Silver Layer (joined 'products' with 'product_category').")


def cleand_payment_method(spark, silver_path, table):
    """
    - The 'bank' column in this table contains a '\r' character at the end of the string, 
      so we need to clean it.
    """
    logger.info(f"[SILVER][{table}] --- Starting transformation for 'payment_method' ---")

    source_df = read_bronze_layer(spark, table="brz.payment_method")
    cleaned_df = source_df.withColumn(
        "bank",
        when(
            col("bank").isin("null", "null\r"),
            lit(None)
        ).otherwise(
            regexp_replace(col("bank"), r"\r$", "")
        )
    )

    cleaned_df.withColumnRenamed("id", "method_id").withColumnRenamed("name", "method_name") \
        .write.mode("overwrite").parquet(f"{silver_path}/{table}")

    logger.info(f"[SILVER][{table}] Successfully wrote records to '{table}' table in Silver Layer.")


def cleaned_customer(spark, silver_path, table):
    logger.info(f"[SILVER][{table}] --- Starting load for 'customers' ---")

    source_df = read_bronze_layer(spark, table="brz.customers")
    source_df.write.mode("overwrite").parquet(f"{silver_path}/{table}")

    logger.info(f"[SILVER][{table}] Successfully wrote records to '{table}' table in Silver Layer.")


def main():
    logger.info("======== [SILVER] START: Loading dimension data from Bronze Layer to Silver Layer ========")
    try:
        spark = create_SparkSession()
        silver_path = f"s3a://silver-layer/"

        cleand_stores(spark, silver_path, table="slv.stores")
        cleand_products(spark, silver_path, table="slv.products")
        cleand_payment_method(spark, silver_path, table="slv.payment_method")
        cleaned_customer(spark, silver_path, table="slv.customers")

        spark.stop()
    except Exception as e:
        logger.error(f"Error during transformation: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()