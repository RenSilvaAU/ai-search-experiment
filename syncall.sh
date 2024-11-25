source .env
python syncall.py \
    --search_service_name $SEARCH_SERVICE_URL \
    --index_name $INDEX_NAME \
    --api_key $SEARCH_API_KEY \
    --connection_string $AZURE_STORAGE_CONNECTION_STRING \
    --container_name $AZURE_STORAGE_CONTAINER_NAME \
    --file_name $AZURE_STORAGE_BLOB_NAME
