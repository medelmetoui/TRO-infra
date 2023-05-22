import logging
import azure.functions as func
import csv
import requests
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime ,timedelta
import pytz

def main(req: func.HttpRequest) -> func.HttpResponse:


    
    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')
    # latest blob
    latest_blob = None
    latest_time = datetime.min
    
    timezone = pytz.timezone('utc')
    latest_time = timezone.localize(latest_time)

    for blob in blobs_list:
        if "DEMANDE_NON_PLANIFIEE" in blob.name:
            if blob.last_modified > latest_time:
                    latest_blob = blob
                    latest_time = blob.last_modified

    # Print the name of the latest blob
    if latest_blob:
        print(f"Latest blob name: {latest_blob.name}")
    else:
        print("No blobs found in the container.")
        return func.HttpResponse("No blobs found in the container.", mimetype="application/json")


    sas = generate_blob_sas(account_name="troblobstorage",
                                container_name="output",
                                blob_name=latest_blob.name,
                                account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==",
                                permission=BlobSasPermissions(read=True,write=True,create=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))

    url = "https://troblobstorage.blob.core.windows.net/output/"+latest_blob.name+"?"+sas
    
    headers = {
        'x-ms-blob-type':'BlockBlob'
    }

    if req.method == 'PUT':
        body = req.get_body().decode('utf-8')
        response = requests.request(req.method,
                                    url,
                                    headers=headers,
                                    data= body,
                                    )
                                    
    if req.method == 'GET':
        response = requests.request(req.method,
                                    url,
                                    headers=headers,
                                    )
        response_text = response.content.decode('utf-8')
        reader = csv.DictReader(response_text.splitlines())
        data = list(reader)
        json_data = json.dumps(data)
        
    
    if response.status_code == 400:
        return func.HttpResponse(
             "Error Acessing the resource (sas)",
        )
    
    else :
        return func.HttpResponse(json_data, mimetype="application/json")