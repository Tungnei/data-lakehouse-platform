import configparser
import json
import os

# Read the config.ini file
config = configparser.ConfigParser()
config.read("config.ini")

# Extract MongoDB connection details from the config file
user = config["mongo"]["username"]
password = config["mongo"]["password"]
cluster = config["mongo"]["cluster"]
database = config["mongo"]["database"]
collection = config["mongo"]["collection"]

# Define the Kafka Connector configuration
connector_config = {
    "name": "mongodb-source",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
        "connection.uri": f"mongodb+srv://{user}:{password}@{cluster}",
        "database": database,
        "collection": collection,
        "topic.prefix": "mongo",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "publish.full.document.only": "true",
        "output.json.formatter": "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson"
    }
}

# Write the generated JSON to a file
current_dir = os.getcwd()
folder_path = os.path.join(current_dir, 'connectors')
filepath = os.path.join(folder_path, "mongodb-source-connector.json")

with open(filepath, "w") as f:
    json.dump(connector_config, f, indent=2)

print("✅ The file mongodb-source-connector.json has been successfully generated!")
