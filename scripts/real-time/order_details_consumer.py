# ================================================================================
# SCRIPT: Tracking ordered products
# ================================================================================
# Description:
#   - Consume messages from Kafka topic 'mysql.coffee_shop.order_details'
#   - Cache each product item into Redis under the key 'ordered_products:{order_id}'
#   - Call check_and_trigger() to verify if all products for an order are present
#   - If complete and order info exists, send a consolidated message to topic
#     'order_ready_for_checking'
# ================================================================================

import logging
import multiprocessing
from pathlib import Path

import redis
from kafka_handler import KafkaHandler

# Logging Configuration
BASE_DIR = Path(__file__).resolve().parent.parent.parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    filename= BASE_DIR / "logs" / "real-time.log",
)
logger = logging.getLogger(__name__)

# Redis Configuration
redis_dynamic = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

def check_and_trigger(order_id, producer):
    if redis_dynamic.get(f"order_status:{order_id}") == "checking":
        return
    
    order_info = redis_dynamic.hgetall(f"order_info:{order_id}")
    if not order_info:
        logging.debug(f"[ORDER_INFO_MISSING] {order_id} waiting for order_info in Redis")
        return
    
    num_products = int(order_info.get("num_products", 0))
    current_products = redis_dynamic.smembers(f"ordered_products:{order_id}")
    if len(current_products) == num_products:
        producer.send("order_ready_for_checking", {
            "order_id": order_id,
            "customer_id": order_info["customer_id"],
            "payment_method_id": order_info["payment_method_id"],
            "product_ids": list(current_products),
        })
    
        redis_dynamic.set(f"order_status:{order_id}", "checking")
        
        redis_dynamic.delete(f"order_info:{order_id}")
        redis_dynamic.delete(f"ordered_products:{order_id}")
    else:
        logging.debug(f"[PRODUCTS_INCOMPLETE] {order_id} (current={len(current_products)}, expected={num_products})")
    return


def process_message(message, producer):
    order_detail_payload = message.value.get("payload")["after"]
    order_id = order_detail_payload["order_id"]
    product_id = order_detail_payload["product_id"]

    if redis_dynamic.get(f"order_status:{order_id}") == "checking":
        return
    
    # Add product_id to the Redis set for this order (to track all ordered products)
    redis_dynamic.sadd(f"ordered_products:{order_id}", product_id)
    redis_dynamic.expire(f"ordered_products:{order_id}", 120)

    check_and_trigger(order_id, producer)


def ordered_products_worker(worker_id: int):
    bootstrap_servers = ["localhost:29092", "localhost:29093"]
    kafka_client = KafkaHandler(bootstrap_servers)
    producer = kafka_client.get_producer()
    consumer = kafka_client.get_consumer(
        topic="mysql.coffee_shop.order_details",
        group_id="order_products_tracker"
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
        logger.info(f"Error processing message: {e}")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        logger.info("Kafka producer closed.")
        logger.info("Kafka consumer closed.")


def main():
    num_workers = 3
    processes = []

    for i in range(num_workers):
        p = multiprocessing.Process(target=ordered_products_worker, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    
if __name__ == "__main__":
    main()
