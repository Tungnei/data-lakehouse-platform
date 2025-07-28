# =======================================================================
# SCRIPT: Check Order Eligibility and Recommend Product
# =======================================================================
# Purpose:
#   - Consume messages from Kafka topic 'order_ready_for_checking'
#   - Check eligibility for diamond-tier customers using ACB bank transfers
#   - Recommend random product with discount offers if conditions are met
# =======================================================================

import random
import logging
import multiprocessing
from pathlib import Path

import redis
from kafka import KafkaProducer
from kafka_handler import KafkaHandler

BASE_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
    filename= BASE_DIR / "logs" / "real-time.log",
)

logger = logging.getLogger(__name__)

redis_static = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
redis_dynamic = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

def is_order_eligible(order_id: str, customer_id: int, payment_method_id: int) -> bool:
    """
    Check if an order is eligible for product recommendation based on:
        - Payment method is bank transfer (ACB)
        - Customer belongs to diamond tier
    Returns:
        Tuple of (eligibility boolean, customer_id if eligible else None)
    """

    if not customer_id or not payment_method_id:
        logger.warning(f"[ELIGIBILITY_FAIL] Order {order_id}: missing customer_id or payment_method_id")
        return False
    
    if not (
        redis_static.sismember("bank_acb_payment", payment_method_id) and
        redis_static.sismember("diamond_customers", customer_id)
    ):
        return False

    return True


def product_recommendation(exclude_ids: list[str]) -> dict:
    """
    Randomly select a product from Redis static cache excluding products in exclude_ids.

    Returns:
        A dictionary with product details or None if no suitable product found.
    """

    keys = redis_static.keys("product:*")
    product_candidates = []

    for key in keys:
        product_id = key.split(":")[1]
        if product_id in exclude_ids:
            continue

        product = redis_static.hgetall(f"product:{product_id}")
        product["product_id"] = product_id
        product_candidates.append(product)

    return random.choice(product_candidates) if product_candidates else None


def process_message(message, producer: KafkaProducer):
    order_payload = message.value
    order_id = order_payload["order_id"]
    customer_id = order_payload["customer_id"]
    payment_method_id = order_payload["payment_method_id"]
    ordered_product_ids = order_payload["product_ids"]
    
    if redis_dynamic.get(f"order_status:{order_id}") == "completed":
        return

    #  --- Start eligibility check for recommendation ---
    # If order does not meet criteria, mark as completed and skip recommendation
    logger.info(f"[PROCESS_START] Checking eligibility for order {order_id}")
    if not is_order_eligible(order_id, customer_id, payment_method_id):
        redis_dynamic.set(f"order_status:{order_id}", "completed", ex=100)
        logger.info(f"[PROCESS_END] Order {order_id} marked completed without recommendation")
        return

    # --- Order passed eligibility checks (diamond tier + ACB bank transfer) ---
    # Proceed to find a recommendation
    exclude_product_ids = [pid for pid in ordered_product_ids]
    suggested_product = product_recommendation(exclude_ids=exclude_product_ids)
    if not suggested_product:
        logger.info(f"[RECOMMEND] Order {order_id}: no suitable product found for recommendation")
        redis_dynamic.set(f"order_status:{order_id}", "completed")
        return

    # --- Prepare suggestion details with random discount ----
    product_id = suggested_product["product_id"]
    product_name = suggested_product["name"]
    unit_price = int(suggested_product["unit_price"])

    discount_factor = round(random.uniform(0.5, 0.8), 2)
    quantity = 1
    subtotal = int(unit_price * quantity * discount_factor)
    discount_percent = round((1 - discount_factor) * 100)

    logger.info(f"[RECOMMEND] Order {order_id}: {product_name} - {discount_percent}% discount")

    producer.send("order_suggestion", {
        "order_id": order_id,
        "product_id": product_id,
        "product_name": product_name,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_percent": discount_percent,
        "subtotal": subtotal,
        "is_suggestion": True
    })

    producer.send("order_suggestion_accepted", {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "discount_percent": discount_percent,
        "subtotal": subtotal,
        "is_suggestion": True
    })
    
    # --- Mark order as completed ---
    redis_dynamic.set(f"order_status:{order_id}", "completed", ex=100)
    logger.info(f"[PROCESS_END] Order {order_id} marked completed with recommendation")


def recommendation_engine_worker(worker_id: int):
    bootstrap_server = ["localhost:29092", "localhost:29093"]
    kafka_client = KafkaHandler(bootstrap_server)
    producer = kafka_client.get_producer()
    consumer = kafka_client.get_consumer(
        topic="order_ready_for_checking",
        group_id="recommendation_engine"
    )

    try:
        while True:
            message_pack = consumer.poll(timeout_ms=1000)
            for _, messages in message_pack.items():
                for message in messages:
                    process_message(message, producer)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Exiting gracefully...")
    except Exception as e:
        logger.info("Error processing message:", e)
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        logger.info("Kafka consumer closed.")
        logger.info("Kafka producer closed.")


def main() -> None:
    num_workers = 2
    processes = []

    for i in range(num_workers):
        p = multiprocessing.Process(target=recommendation_engine_worker, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
