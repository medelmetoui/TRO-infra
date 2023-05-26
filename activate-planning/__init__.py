import logging

import azure.functions as func
import requests
import csv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import timedelta,datetime
import json

def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')


    planning_name_to_activate = req.get_body().decode('utf-8')


    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC/META"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')
    
    all_meta = []

    for blob in blobs_list:
        blob_client=container_client.get_blob_client(blob)
        blob_content = blob_client.download_blob().content_as_text()
        meta_file = json.loads(blob_content)

        if meta_file['active'] == True:
            meta_file['active']=False
            
        if meta_file['planning_filename'] == planning_name_to_activate:
            meta_file['active']=True

        blob_client.upload_blob(json.dumps(meta_file),overwrite=True)
        

    return func.HttpResponse(f"plannning{planning_name_to_activate} is activated")
    
    
    