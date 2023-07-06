import logging
import azure.functions as func
import csv
import requests
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions,BlobClient
from datetime import datetime ,timedelta
import pytz
import os 



def main(req: func.HttpRequest) -> func.HttpResponse:

    repository_name="SEC"

    # active planning 
    unscheduled_file_name = "UNSCHEDULED_QUEUE.json"

    ## FROM META WE GET THE ACTIVE PLANNING 
    sas = generate_blob_sas(account_name="troblobstorage",
                                container_name="output",
                                blob_name=repository_name+"/"+unscheduled_file_name,
                                account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==",
                                permission=BlobSasPermissions(read=True,write=True,create=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))

    url = "https://troblobstorage.blob.core.windows.net/output/"+repository_name+"/"+unscheduled_file_name+"?"+sas

    blob_client = BlobClient.from_blob_url(url)
    ## Compare new planning bel 9dim 
    blob_data = blob_client.download_blob()
    blob_content = blob_data.content_as_text()
    unscheduled_file = json.loads(blob_content)
    
    if unscheduled_file:
        if req.method == 'GET':
            return func.HttpResponse(json.dumps(unscheduled_file),mimetype="application/json")
    else:
        return func.HttpResponse(json.dumps([]),mimetype="application/json")

