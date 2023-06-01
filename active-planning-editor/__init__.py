import logging

import azure.functions as func
import requests
import json
import datetime
import csv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import os 
from utils import (
    remove_array ,
    transform_format
    )

def get_old_trajet(planning,agent,shift):

    trajet= []

    positions = []

    for idx,elem in enumerate(planning):
        if elem['agent']==agent and elem['Plage horaire']==shift:
            trajet.append(elem)
            positions.append(idx)
    
    return trajet,positions


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    response = requests.get('https://last-planning.azurewebsites.net/api/active-planning')
    planning = response.json()

    response_2 = requests.get('https://last-planning.azurewebsites.net/api/active-planning-details')
    meta_planning = response_2.json()

    blob_name = meta_planning['planning_filename']

    if response.status_code == 200:

        if req.method == 'PUT':

            # body on retrouve le trajet 

            new_trajet = req.get_json()

            for state in new_trajet:
                agent_to_edit = state['agent']
                shift = state['Plage horaire']
            
            old_trajet,positions= get_old_trajet(planning,agent_to_edit,shift)

            new_planning = remove_array(planning,old_trajet)

            for idx,elem in enumerate(planning):
                if idx == positions[0]:
                    #new_planning[idx].update(new_trajet)
                    for i, etat in enumerate(new_trajet):
                        new_planning[idx + i] = etat
                        
                    # for state in new_trajet:
                    #     new_planning.insert(idx,state)
            
            data = transform_format(new_planning)

            connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
            container_name = 'output'

            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_client = blob_service_client.get_container_client(container_name)
            repository_name="SEC"
            blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')
            
            for blob in blobs_list:
                csv_name = os.path.basename(blob.name)
                if csv_name == blob_name:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
                    blob_client.upload_blob(data, overwrite=True)

                    return func.HttpResponse(f"plannning{blob.name} is updated")






