import logging

import azure.functions as func
import requests
import csv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import timedelta,datetime
import json
import re

# def get_all_routes(planning)->list():

#     all_routes = []

#     for idx,step in enumerate(planning):

#         if step['etat'] == "Chargement":
#             route['idx_began']=idx
#             route['agent']= step['agent']

#         if step['etat'] == "Fin" and route['agent']==step['agent']:
#             route['idx_end']=idx
#             all_routes.append(route)
    
#     return all_routes



## for each line in the planning nekhou status men chargement w ekef hata lin fama fin => 3ana route
## 

# def validate_route_by_agent(route,planning):
#     """
#     we check if the route is valid by agent
#     """
#     agent = route['agent']
#     idx_began = route['idx_began']
#     idx_end = route['idx_end']

#     for idx,step in enumerate(planning[idx_began:idx_end]):
#         if step['agent'] != agent:
#             return False
#     return True




def main(req: func.HttpRequest) -> func.HttpResponse:

    """
    from a req we check if we are going to validate a planning or only one route
    if it's one route we will have : req.body == "1784 TU 182" \d+\W?TU\W?\d+
    if it's a whole planning, we must check that all the routes are validated, then validate the planning : req.body == activeplanning_name

    TODO : Implement a better logic to handle the req.body
    """

    logging.info('Python HTTP trigger function processed a request.')
    planning_name_to_validate = req.get_body().decode('utf-8')

    ### generate all routes

    response = requests.get('https://last-planning.azurewebsites.net/api/active-planning')
    planning = response.json()

    #routes = get_all_routes(planning)

    response_2 = requests.get('https://last-planning.azurewebsites.net/api/active-planning-details')
    meta_planning = response_2.json()

    

    # routes = get_all_routes(planning)





    # if response.status_code == 200 and response_2.status_code == 200:
    #     meta_planning['routes'] = routes
    #     logging.info('Successful request')
    #     return func.HttpResponse(planning)

    # connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    # container_name = 'output'
    # blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # container_client = blob_service_client.get_container_client(container_name)
    # repository_name="SEC/META"
    # blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')
    
    # all_meta = []

    # for blob in blobs_list:
    #     blob_client=container_client.get_blob_client(blob)
    #     blob_content = blob_client.download_blob().content_as_text()
    #     meta_file = json.loads(blob_content)
            
    #     if meta_file['planning_filename'] == planning_name_to_validate:
    #         meta_file['status']="Validated"

    #     blob_client.upload_blob(json.dumps(meta_file),overwrite=True)
        

    # return func.HttpResponse(f"plannning{planning_name_to_validate} is activated")
    
    
    