import logging

import azure.functions as func
import requests
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions,BlobClient
import os 
import aiohttp
from datetime import datetime,timedelta
import asyncio
from retrying import retry
from cachetools import cached, TTLCache,LRUCache
from time import process_time
import re
from jsondiff import diff
from deepdiff import DeepDiff


async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    body = req.get_json()

    blob_name = str(body['planning_name'])
    planning = json.dumps(body['planning'])
    repository_name="SEC"
    sas = generate_blob_sas(
        account_name="troblobstorage",
        container_name="output",
        blob_name=repository_name+"/" +blob_name,
        account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==", 
        permission=BlobSasPermissions(read=True, write=True, create=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )

    # Create the SAS URL
    url = "https://troblobstorage.blob.core.windows.net/output/"+repository_name+"/" + blob_name + "?" + sas    

    blob_client = BlobClient.from_blob_url(url)
    ## Compare new planning bel 9dim 
    blob_data = blob_client.download_blob()
    blob_content = blob_data.content_as_text()
    old_planning = json.loads(blob_content)

    new_planning = json.loads(planning)
    json_diff= DeepDiff(old_planning,new_planning)

    if json_diff:
        # if "startTime" in json_diff.affected_paths.items[0]:
            
        #     path = json_diff.affected_paths.items[0]        
        #     pattern = r"\[(\d+)\]"
        #     matches = re.findall(pattern, path)

        #     if len(matches) >= 2:
        #         edited_trajet_id = int(matches[0])
        #         step_id = int(matches[1])

        #     for trajet in old_planning:
        #         if trajet['trajet_id']==edited_trajet_id:
        #             for idx, step in enumerate(trajet['steps']):
        #                 if idx ==0:
        #                     old_start_time_str= step['startTime']
        #                     old_start_time = datetime.strptime(old_start_time_str, "%Y-%m-%dT%H:%M:%S")

        #     for trajet in new_planning:
        #         if trajet['trajet_id']==edited_trajet_id:
        #             for idx, step in enumerate(trajet['steps']):    
        #                 if idx ==0:
        #                     new_start_time_str= step['startTime']
        #                     new_start_time = datetime.strptime(new_start_time_str, "%Y-%m-%dT%H:%M:%S")

        #     duration_diff=  old_start_time-new_start_time
        #     if duration_diff.total_seconds()>0:
        #         for trajet in new_planning:
        #             for idx,step in enumerate(trajet['steps']):
        #                 if idx==0:
        #                     end_time = step['endTime']
        #                     end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
        #                     end = end + duration_diff
        #                     time_str_reconverted = end.strftime("%Y-%m-%dT%H:%M:%S")
        #                     step['endTime'] = time_str_reconverted
        #                 else:
        #                     start_time = step['startTime']
        #                     start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
        #                     start = start + duration_diff
        #                     start_time_str_reconverted = start.strftime("%Y-%m-%dT%H:%M:%S")
        #                     step['startTime'] =start_time_str_reconverted

        #                     end_time = step['endTime']
        #                     end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
        #                     end = end + duration_diff
        #                     end_time_str_reconverted = end.strftime("%Y-%m-%dT%H:%M:%S")
        #                     step['endTime'] = end_time_str_reconverted    
                            
        #     if duration_diff.total_seconds()<0:
        #         for trajet in new_planning:
        #             for idx,step in enumerate(trajet['steps']):
        #                 if idx==0:
        #                     end_time = step['endTime']
        #                     end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
        #                     end = end - duration_diff
        #                     time_str_reconverted = end.strftime("%Y-%m-%dT%H:%M:%S")
        #                     step['endTime'] = time_str_reconverted
        #                 else:
        #                     start_time = step['startTime']
        #                     start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
        #                     start = start - duration_diff
        #                     start_time_str_reconverted = start.strftime("%Y-%m-%dT%H:%M:%S")
        #                     step['startTime'] =start_time_str_reconverted

        #                     end_time = step['endTime']
        #                     end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
        #                     end = end - duration_diff
        #                     end_time_str_reconverted = end.strftime("%Y-%m-%dT%H:%M:%S")
        #                     step['endTime'] = end_time_str_reconverted   

        blob_client.upload_blob(json.dumps(new_planning),overwrite=True)

    return func.HttpResponse(json.dumps(new_planning), mimetype="application/json")
    
    # try:
    #     start = timer()

    #     urls = [
    #     'https://last-planning.azurewebsites.net/api/active-planning',
    #     'https://last-planning.azurewebsites.net/api/active-planning-details'
    #     ]         

    #     tasks = [fetch(url) for url in urls]
    #     results = await asyncio.gather(*tasks)
     

    # except Exception as e:
    #     logging.error(e)
    #     return func.HttpResponse(
    #         "Request error",
    #         status_code=500
    #     )
    
    # end = timer()
    # el_1 = end -start

    

    # all_trajets = results[0]
    # meta_planning = results[1]

    # blob_name = meta_planning['planning_filename']

    # if all_trajets: # TODO : replace with status code

    #     if req.method == 'PUT':
            

    #         # body on retrouve le trajet 

    #         new_trajet = req.get_json()

    #         for trajet in all_trajets:
    #             if trajet['trajet_id'] == new_trajet['trajet_id']:
    #                 trajet.update(new_trajet)
            
    #         data = json.dumps(all_trajets)

    #         repository_name="SEC"
    #         sas = generate_blob_sas(
    #             account_name="troblobstorage",
    #             container_name="output",
    #             blob_name=repository_name+"/" +blob_name,
    #             account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==", 
    #             permission=BlobSasPermissions(read=True, write=True, create=True),
    #             expiry=datetime.utcnow() + timedelta(hours=1)
    #         )

    #         # Create the SAS URL
    #         url = "https://troblobstorage.blob.core.windows.net/output/"+repository_name+"/" + blob_name + "?" + sas    

    #         blob_client = BlobClient.from_blob_url(url)  
    #         blob_client.upload_blob(data,overwrite=True)

    #         end_time = process_time()
    #         res = end_time - start_time
            
    #         return func.HttpResponse(f"plannning{blob_name}is updated in :"+str(res))    

