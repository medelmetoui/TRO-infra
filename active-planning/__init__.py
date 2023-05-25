import logging
import azure.functions as func
import csv
import requests
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import timedelta,datetime
import os
import pytz

def transform_format(input_data):
    transformed_data = []
    transformed_data.append("jour,Plage horaire,governorate,agent,agent_capcity,Magasin,LATITUDE,LONGITUDE,etat,quantity,duration,distance,startTime,endTime")
    
    for item in input_data:
        transformed_item = [
            item["jour"],
            item["Plage horaire"],
            item["governorate"],
            item["agent"],
            item["agent_capcity"],
            item["Magasin"],
            item["LATITUDE"],
            item["LONGITUDE"],
            item["etat"],
            item["quantity"],
            item["duration"],
            item["distance"],
            item["startTime"],
            item["endTime"]
        ]
        transformed_data.append(",".join(transformed_item))
    
    return "\n".join(transformed_data)

def remove_array(main_array, array_to_remove):
    return [x for x in main_array if x not in array_to_remove]

def get_active_planning():

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
        else:
            return "No active planning"
    
def main(req: func.HttpRequest) -> func.HttpResponse:

    connect_str = 'DefaultEndpointsProtocol=https;AccountName=troblobstorage;AccountKey=YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==;EndpointSuffix=core.windows.net'
    container_name = 'output'

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    repository_name="SEC"
    blobs_list = container_client.list_blobs(name_starts_with=repository_name + '/')

    # active planning 
    active_planning = get_active_planning()
    active_planning_name = active_planning['planning_filename']

    ## FROM META WE GET THE ACTIVE PLANNING 
    
    for blob in blobs_list:
        blob_name = os.path.basename(blob.name)
        if active_planning_name == blob_name:            
            active_planning  = blob
            continue

    # Print the name of the latest blob
    if active_planning:
        logging.info(f"Active blob name: {active_planning.name}")

        sas = generate_blob_sas(account_name="troblobstorage",
                                    container_name="output",
                                    blob_name=active_planning.name,
                                    account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==",
                                    permission=BlobSasPermissions(read=True,write=True,create=True),
                                    expiry=datetime.utcnow() + timedelta(hours=1))

        url = "https://troblobstorage.blob.core.windows.net/output/"+active_planning.name+"?"+sas
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-ms-blob-type':'BlockBlob'
        }


        if req.method == 'PUT':

            # custom edit par agent trajet
            body = req.get_json()

            #body = transform_format(body)

            response = requests.request("GET",
                                        url,
                                        headers=headers,
                                        )
            
            response_text = response.content.decode('utf-8')
            reader = csv.DictReader(response_text.splitlines())
            data = list(reader)

            agent_to_edit = body[0]['agent']
            trajet_to_edit = []

            ### conservation de la duree du trajet:

            new_start_time_str = body[0]['startTime']
            
            for elem in data:
                if elem['agent'] == agent_to_edit:
                    trajet_to_edit.append(elem)

            old_start_time_str = trajet_to_edit[0]['startTime']

            new_start_time = datetime.strptime(new_start_time_str, "%Y-%m-%dT%H:%M:%S")
            old_start_time = datetime.strptime(old_start_time_str, "%Y-%m-%dT%H:%M:%S")

            duration_diff=  abs(old_start_time-new_start_time)

            ### add duration diff to the rest of times

            for idx,elem in enumerate(body):
                if idx==0:
                    end_time = elem['endTime']
                    end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
                    end = end + duration_diff
                    time_str_reconverted = end.strftime("%Y-%m-%dT%H:%M:%S")
                    elem['endTime'] = time_str_reconverted

                else:
                    start_time = elem['startTime']
                    start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
                    start = start + duration_diff
                    start_time_str_reconverted = start.strftime("%Y-%m-%dT%H:%M:%S")
                    elem['startTime'] =start_time_str_reconverted

                    end_time = elem['endTime']
                    end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
                    end = end + duration_diff
                    end_time_str_reconverted = end.strftime("%Y-%m-%dT%H:%M:%S")
                    elem['endTime'] = end_time_str_reconverted
                    
            ######################################################################

            
            data =  remove_array(data, trajet_to_edit)

            for elem in body:
                data.append(elem)
            
            # data.update(body)


            output = transform_format(data)
            
            response =  requests.request(req.method,
                                        url,
                                        headers=headers,
                                        data=output
                                        )
            response_text = response.content.decode('utf-8')
            reader = csv.DictReader(response_text.splitlines())
            data = list(reader)
            json_data = json.dumps(data)
        
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
    else:
        logging.info("No active planning found.")
        return func.HttpResponse("No activated planning found in the container.", mimetype="application/json")

