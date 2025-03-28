Write-Host "--> Generate the mongodb-source-connector.json file"
python connectors/generate-mongo-connector.py
Write-Host "===================================================="

Start-Sleep -Seconds 5
Write-Host "--> Registering MongoDB Source Connector..."
Invoke-WebRequest -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile ".\connectors\mongodb-source-connector.json"
Write-Host "===================================================="

Start-Sleep -Seconds 5
Write-Host "--> Registering Elasticsearch Sink Connector..."
Invoke-WebRequest -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -InFile ".\connectors\elasticsearch-sink-connector.json"
Write-Host "===================================================="

Start-Sleep -Seconds 5
Write-Host "--> Generating data ..."
python scripts/mongodb_data.py
Write-Host "===================================================="

Start-Sleep -Seconds 5
Write-Host "--> Checking MongoDB source connector status..."
Invoke-RestMethod -Uri "http://localhost:8083/connectors/mongodb-source/status"
Write-Host "===================================================="

Start-Sleep -Seconds 5
Write-Host "--> Checking Elasticsearch sink connector status..."
Invoke-RestMethod -Uri "http://localhost:8083/connectors/elasticsearch-sink/status"
Write-Host "===================================================="

Write-Host "--> If nothing goes wrong, can manual sync Airbyte https://cloud.airbyte.com/workspaces/6abdf744-0697-44b8-9e50-5ef7cabed21e/connections ..."