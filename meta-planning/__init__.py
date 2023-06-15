import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import json

def update_active_attribute(blob_service_connection_string, container_name,repository_name, file_name):

    blob_service_client = BlobServiceClient.from_connection_string(blob_service_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Get a list of blob names in the container
    blob_names = [blob.name for blob in container_client.list_blobs(name_starts_with=repository_name + '/')]

    # Update the active attribute in the specified meta file
    updated_blob_name = None
    for blob_name in blob_names:
        if blob_name.endswith(file_name):
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            meta_data = json.loads(blob_data)
            if meta_data["active"] == True:
                # Already active, no need to update
                return
            meta_data["active"] = True
            updated_blob_name = blob_name
            updated_blob_data = json.dumps(meta_data)
            blob_client.upload_blob(updated_blob_data, overwrite=True)
            continue
            
        else:
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            meta_data = json.loads(blob_data)
            if meta_data["active"] == False:
                # Already unactive, no need to update
                continue
            else:
                meta_data["active"] = False
                updated_blob_name = blob_name
                updated_blob_data = json.dumps(meta_data)
                blob_client.upload_blob(updated_blob_data, overwrite=True)
                continue
                


    if updated_blob_name is not None:
        print(f"Updated active attribute in blob: {updated_blob_name}")
    else:
        print("No meta file found with the specified file name.")





def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')

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
        json_data = json.loads(blob_content)
        all_meta.append(json_data)
    
    if req.method=="PUT":
        meta_to_update  = req.get_json()
        file_name = meta_to_update['planning_filename']
        update_active_attribute(connect_str,container_name,repository_name,file_name)


    if all_meta:
        json_data = json.dumps(all_meta)
        return func.HttpResponse(json_data, mimetype="application/json")
    
    else:
        return func.HttpResponse("No metadata found")

