import logging
import azure.functions as func
import csv
import requests
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions,BlobClient
from datetime import timedelta,datetime
import os
import pytz

def get_active_planning_meta():

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC/META"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')
    
    active_planning = {}

    ## TRY CATCH NO ACTIVE PLANNING

    for blob in blobs_list:
        blob_client=container_client.get_blob_client(blob)
        blob_content = blob_client.download_blob().content_as_text()
        file=json.loads(blob_content)
        if file['active']:
            active_planning = file
            return active_planning
 
    
def main(req: func.HttpRequest) -> func.HttpResponse:

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')

    # active planning 
    active_planning_meta = get_active_planning_meta()
    active_planning_name = active_planning_meta['planning_filename']

    ## FROM META WE GET THE ACTIVE PLANNING 
    sas = generate_blob_sas(account_name="troblobstorage",
                                container_name="output",
                                blob_name=repository_name+"/"+active_planning_name,
                                account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==",
                                permission=BlobSasPermissions(read=True,write=True,create=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))

    url = "https://troblobstorage.blob.core.windows.net/output/"+repository_name+"/"+active_planning_name+"?"+sas

    blob_client = BlobClient.from_blob_url(url)
    ## Compare new planning bel 9dim 
    blob_data = blob_client.download_blob()
    blob_content = blob_data.content_as_text()
    active_planning_blob_file = json.loads(blob_content)
    
    if active_planning_blob_file:
        if req.method == 'GET':
            return func.HttpResponse(json.dumps(active_planning_blob_file),mimetype="application/json")
