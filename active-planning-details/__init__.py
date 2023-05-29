import logging

import azure.functions as func
import os
import json
from azure.storage.blob import BlobServiceClient


def get_active_planning():

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC/META"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')
    
    active_planning = {}

    ## TODO TRY CATCH NO ACTIVE PLANNING

    for blob in blobs_list:
        blob_client=container_client.get_blob_client(blob)
        blob_content = blob_client.download_blob().content_as_text()
        file=json.loads(blob_content)
        if file['active']:
            active_planning = file
            return active_planning
 
    
def main(req: func.HttpRequest) -> func.HttpResponse:

    # active planning 
    active_planning = get_active_planning()

    # Print the name of the latest blob
    if active_planning:
        logging.info(f"Active planning meta name: {active_planning['planning_filename']}")
        return func.HttpResponse(json.dumps(active_planning), mimetype="application/json")
    else:
        return func.HttpResponse("No planning activated please generate a planning", mimetype="application/json")
