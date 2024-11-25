import argparse
import requests
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from io import StringIO

# Step 1: Perform a full search to get all documents in the index
def search_index(base_url, index_name, api_key):
    """
    Queries an Azure Cognitive Search index to retrieve all documents.
    
    Args:
        base_url (str): The base URL of the Azure Cognitive Search service.
        index_name (str): The name of the search index.
        api_key (str): The API key for authenticating requests to the search service.
        
    Returns:
        dict: A dictionary where the keys are `id` and the values are `content` for each document.
        Returns an empty dictionary if there is an error.
    """
    search_url = f"{base_url}/indexes/{index_name}/docs/search?api-version=2024-07-01"
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    query = {
        "search": "*",
        "select": "id, content"
    }
    
    # Make the search request
    response = requests.post(search_url, headers=headers, json=query)
    
    if response.status_code == 200:
        # Parse and return search results
        search_results = response.json()
        return {doc["id"]: doc["content"] for doc in search_results["value"]}
    else:
        # Handle any errors
        print(f"Error during search: {response.status_code}, {response.text}")
        return {}

# Step 2: Load CSV file from Azure Blob Storage
def load_csv_from_blob(connection_string, container_name, blob_name):
    """
    Downloads and reads a CSV file from Azure Blob Storage, and converts it to a dictionary.
    
    Args:
        connection_string (str): Connection string to the Azure Blob Storage account.
        container_name (str): The name of the Blob Storage container.
        blob_name (str): The name of the CSV file in the container.
        
    Returns:
        dict: A dictionary where the keys are `id` and the values are `content` for each record in the CSV.
    """
    # Create BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    # Download the blob's content as a string
    download_stream = blob_client.download_blob()
    csv_data = download_stream.readall().decode("utf-8")
    
    # Load CSV into pandas DataFrame from the string content
    df = pd.read_csv(StringIO(csv_data))
    
    # Convert CSV into a dictionary with `id` as key
    return {f'{row["id"]}': row["content"] for _, row in df.iterrows()}

# Step 3: Delete records from the index that are not in the CSV
def delete_documents_from_index(base_url, index_name, api_key, doc_ids_to_delete, index_data):
    """
    Deletes documents from the Azure Cognitive Search index based on provided `id`s.
    
    Args:
        base_url (str): The base URL of the Azure Cognitive Search service.
        index_name (str): The name of the search index.
        api_key (str): The API key for authenticating requests to the search service.
        doc_ids_to_delete (list): A list of `id`s that need to be deleted from the index.
        index_data (dict): Current data in the index, with `id` as key and `content` as value.
        
    Returns:
        None
    """
    delete_url = f"{base_url}/indexes/{index_name}/docs/index?api-version=2024-07-01"
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    
    # Log the documents to be deleted
    print("Deleting the following documents:")
    for doc_id in doc_ids_to_delete:
        content = index_data.get(doc_id, "Content not available")
        print(f"ID: {doc_id}, {content[:30]}...")
    
    # Prepare the data for deletion
    delete_data = {
        "value": [{"@search.action": "delete", "id": doc_id} for doc_id in doc_ids_to_delete]
    }

    # Send the delete request to Azure Cognitive Search
    response = requests.post(delete_url, headers=headers, json=delete_data)
    
    if response.status_code == 200:
        print(f"Successfully deleted {len(doc_ids_to_delete)} documents.")
    else:
        print(f"Error during deletion: {response.status_code}, {response.text}")

# Main function
def main(args):
    """
    Main process to search for documents in the Azure Cognitive Search index, compare with records from a CSV file,
    and delete records that are missing from the index.
    
    Args:
        args (argparse.Namespace): The command-line arguments passed to the script, including the search service details,
                                   and Azure Blob Storage connection information.
                                   
    Returns:
        None
    """
    # Construct the base URL for the Azure Cognitive Search service
    base_url = f"https://{args.search_service_name}.search.windows.net"
    
    # Get all records from the search index
    index_data = search_index(base_url, args.index_name, args.api_key)
    
    # Load the CSV data from Azure Blob Storage
    csv_data = load_csv_from_blob(args.connection_string, args.container_name, args.file_name)
    
    # Find records in the CSV that are not in the index
    missing_ids = [doc_id for doc_id in index_data if doc_id not in csv_data]

    # If there are missing records, delete them from the index
    if missing_ids:
        delete_documents_from_index(base_url, args.index_name, args.api_key, missing_ids, index_data)
    else:
        print("No documents to delete. All records in the CSV are present in the index.")

# Run the main function when the script is executed
if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Delete missing records from Azure Cognitive Search index based on a CSV file in Azure Blob Storage.")
    
    # Add arguments to be passed to the script
    parser.add_argument('--search_service_name', required=True, help='Azure Cognitive Search service name')
    parser.add_argument('--index_name', required=True, help='Name of the Azure Cognitive Search index')
    parser.add_argument('--api_key', required=True, help='API key for the Azure Cognitive Search service')
    parser.add_argument('--connection_string', required=True, help='Connection string for the Azure Blob Storage account')
    parser.add_argument('--container_name', required=True, help='Name of the Azure Blob Storage container')
    parser.add_argument('--file_name', required=True, help='Name of the CSV file in the Blob Storage container')

    # Parse arguments and run the main function
    args = parser.parse_args()
    main(args)
