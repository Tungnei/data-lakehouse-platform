from pymongo import MongoClient
from datetime import datetime
import random, faker
import configparser

fake = faker.Faker()
config = configparser.ConfigParser()
config.read("config.ini")

# Connect to MongoDB
username = config["mongo"]["username"]
password = config["mongo"]["password"]
cluster = config["mongo"]["cluster"]
database = config["mongo"]["database"]

uri = f"mongodb+srv://{username}:{password}@{cluster}"
client = MongoClient(uri)
db = client[database]

products = [
    # Coffee
    {"product_id": "CF01", "name": "Black Coffee", "category": "Coffee", "price": 30000},
    {"product_id": "CF02", "name": "Condensed Milk Coffee", "category": "Coffee", "price": 35000},
    {"product_id": "CF03", "name": "Bac Xiu", "category": "Coffee", "price": 40000},
    {"product_id": "CF04", "name": "Egg Coffee", "category": "Coffee", "price": 45000},
    {"product_id": "CF05", "name": "Cold Brew", "category": "Coffee", "price": 50000},
    {"product_id": "CF06", "name": "Cold Brew Orange", "category": "Coffee", "price": 55000},
    {"product_id": "CF07", "name": "Cold Brew Lemon", "category": "Coffee", "price": 55000},
    # Tea
    {"product_id": "T01", "name": "Peach Tea", "category": "Tea", "price": 45000},
    {"product_id": "T02", "name": "Lychee Tea", "category": "Tea", "price": 45000},
    {"product_id": "T03", "name": "Jasmine Tea with Creamcheese", "category": "Tea", "price": 50000},
    {"product_id": "T04", "name": "Oolong Milk Tea", "category": "Tea", "price": 45000},
    # Yogurt
    {"product_id": "Y01", "name": "Plain Yogurt", "category": "Yogurt", "price": 40000},
    {"product_id": "Y02", "name": "Strawberry Yogurt", "category": "Yogurt", "price": 45000},
    {"product_id": "Y03", "name": "Blueberry Yogurt", "category": "Yogurt", "price": 45000},
    {"product_id": "Y04", "name": "Mango Yogurt", "category": "Yogurt", "price": 45000},
    # Juice
    {"product_id": "J01", "name": "Orange Juice", "category": "Juice", "price": 45000},
    {"product_id": "J02", "name": "Watermelon Juice", "category": "Juice", "price": 45000},
    {"product_id": "J03", "name": "Guava Juice", "category": "Juice", "price": 45000},
    {"product_id": "J04", "name": "Apple Juice", "category": "Juice", "price": 45000},
    # Cake
    {"product_id": "C01", "name": "Croissant", "category": "Cake", "price": 40000},
    {"product_id": "C02", "name": "Salted Egg Croissant", "category": "Cake", "price": 45000},
    {"product_id": "C03", "name": "Tiramisu", "category": "Cake", "price": 55000},
    {"product_id": "C04", "name": "Passion Fruit Mousse", "category": "Cake", "price": 50000},
    {"product_id": "C05", "name": "Matcha Cheesecake", "category": "Cake", "price": 50000},
]

# Stimulate many stores
store_ids = [1, 2, 3, 4, 5, 6]

# Create a random transaction
def generate_transaction():
    num_items=random.choices([1,2,3,4,5], weights=[0.65,0.15,0.1,0.05,0.05])[0]
    selected_items = random.sample(products, num_items)
    items = []

    total_amount = 0
    for item in selected_items:
        quantity = random.choices([1, 2, 3], weights=[0.85, 0.1, 0.05])[0]
        subtotal = item['price'] * quantity
        items.append({
            "product_id": item['product_id'],
            'name': item['name'],
            'category': item['category'],
            'quantity': quantity,
            'subtotal': subtotal
        })

        total_amount += subtotal

    return {
        "timestamp": datetime.now().isoformat(),
        "store_id": random.choice(store_ids),
        "total_amount": total_amount,
        "payment_method": random.choices([1, 2, 3], weights=[0.5,0.05,0.45])[0],
        "currency": "VND",
        "items": items
    }


# Create random number of transactions
num_transaction = random.choices([1,2,3,4,5], weights=[0.7,0.1,0.1,0.05,0.05])[0]
transactions = [generate_transaction() for _ in range(num_transaction)]

db.transactions.insert_many(transactions)

print(f"{num_transaction} transaction(s) at {datetime.now()}")