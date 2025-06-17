Set-Location -Path "C:\Users\ASUS\Project\coffee-sales-project"

# Run the script to create necessary tables in the MySQL database
python scripts/database/create_table.py
Write-Host "============================================================"

# Load static reference data (e.g., products, stores) into the database
python scripts/database/load_static_file.py
Write-Host "============================================================"

# Populate Redis cache with referential data for faster lookups
python scripts/database/lookup_data_cache.py
Write-Host "============================================================"

# Register the MySQL source connector to Kafka Connect for capturing change data (CDC)
Invoke-WebRequest -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile "./scripts/real-time/mysql-src-connector.json"
Write-Host "============================================================"

# Start the Kafka client to listen for new order events and handle product suggestions logic
python scripts/real-time/kafka_client.py