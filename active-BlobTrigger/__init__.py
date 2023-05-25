import logging

import azure.functions as func


from azure.storage.blob import BlobServiceClient

import json
import os 

def main(myblob: func.InputStream):

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC/META"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')

    new_blob = myblob.read()
    new_file=json.loads(new_blob)

    ### Edit the active attribute except for the newly uploaded planning

    if(new_file['active']==True):
        for blob in blobs_list:
            name_in_blob = os.path.basename(blob.name)
            name_in_process = os.path.basename(myblob.name)
            if name_in_blob == name_in_process:
                continue
            else:   
                blob_client=container_client.get_blob_client(blob)
                blob_content = blob_client.download_blob().content_as_text()
                file=json.loads(blob_content)
                if file['active']==True:
                    file['active']=False
                    updated_content = json.dumps(file)
                    blob_client.upload_blob(updated_content, overwrite=True)


    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    
