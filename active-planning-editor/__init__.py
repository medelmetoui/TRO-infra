import logging

import azure.functions as func
import json
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions,BlobClient
from datetime import datetime,timedelta
from deepdiff import DeepDiff
import re

def trajet_steps_update(planning):
    for trajet in planning:
        trajet_starts_at_first_step =trajet['steps'][0]['DEBUT']
        trajet_ends_at_last_step = trajet['steps'][-1]['FIN']
        trajet['trajet_start_time']=trajet_starts_at_first_step
        trajet['trajet_end_time']=trajet_ends_at_last_step
    return planning


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
        if "DEBUT" in json_diff.affected_paths.items[0] and "steps" in json_diff.affected_paths.items[0] :
            
            path = json_diff.affected_paths.items[0]        
            pattern = r"\[(\d+)\]"
            matches = re.findall(pattern, path)

            if len(matches) >= 2:
                edited_trajet_id = int(matches[0])
                step_id = int(matches[1])

            for trajet in old_planning:
                if trajet['trajet_id']==edited_trajet_id:
                    for idx, step in enumerate(trajet['steps']):
                        if idx ==0:
                            old_start_time_str= step['DEBUT']
                            old_start_time = datetime.strptime(old_start_time_str, "%Y-%m-%d %H:%M")

            for trajet in new_planning:
                if trajet['trajet_id']==edited_trajet_id:
                    for idx, step in enumerate(trajet['steps']):    
                        if idx ==0:
                            new_start_time_str= step['DEBUT']
                            new_start_time = datetime.strptime(new_start_time_str, "%Y-%m-%d %H:%M")

            if old_start_time<new_start_time:
                duration_diff=  new_start_time-old_start_time
                ## On additionne la difference 
                for trajet in new_planning:
                    if trajet['trajet_id']==edited_trajet_id:
                        for idx,step in enumerate(trajet['steps']):
                            if idx==0:
                                end_time = step['FIN']
                                end = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
                                end = end + duration_diff
                                start_time_str_reconverted = end.strftime("%Y-%m-%d %H:%M")
                                step['FIN'] = start_time_str_reconverted
                                trajet['trajet_start_time']=start_time_str_reconverted
                            else:
                                start_time = step['DEBUT']
                                start = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
                                start = start + duration_diff
                                start_time_str_reconverted = start.strftime("%Y-%m-%d %H:%M")
                                step['DEBUT'] =start_time_str_reconverted

                                end_time = step['FIN']
                                end = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
                                end = end + duration_diff
                                end_time_str_reconverted = end.strftime("%Y-%m-%d %H:%M")
                                step['FIN'] = end_time_str_reconverted  
                        
                        # trajet_starts_at_first_step =trajet['steps'][0]['startTime']
                        # trajet_ends_at_last_step = trajet['steps'][-1]['endTime']
                        # trajet['trajet_start_time']=trajet_starts_at_first_step
                        # trajet['trajet_end_time']=trajet_ends_at_last_step

                        # # Calcul nouvelle duree

                        # traj_start = datetime.strptime(trajet['trajet_start_time'], "%Y-%m-%d %H:%M")
                        # traj_end = datetime.strptime(trajet['trajet_end_time'], "%Y-%m-%d %H:%M")
                        # total_diff= traj_end-traj_start
                        # formatted_duration = f"{total_diff.seconds // 3600:02d}:{(total_diff.seconds // 60) % 60:02d}:{total_diff.seconds % 60:02d}"

                        # trajet['duree du trajet']=formatted_duration
                        
                    else:
                        continue 
                                
            if new_start_time<old_start_time:
                duration_diff=  old_start_time-new_start_time
                ## On diminue la difference
                for trajet in new_planning:
                    if trajet['trajet_id']==edited_trajet_id:
                        ## Update steps
                        for idx,step in enumerate(trajet['steps']):
                            if idx==0:
                                end_time = step['FIN']
                                end = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
                                end = end - duration_diff
                                time_str_reconverted = end.strftime("%Y-%m-%d %H:%M")
                                step['FIN'] = time_str_reconverted
                            else:
                                start_time = step['DEBUT']
                                start = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
                                start = start - duration_diff
                                start_time_str_reconverted = start.strftime("%Y-%m-%d %H:%M")
                                step['DEBUT'] =start_time_str_reconverted

                                end_time = step['FIN']
                                end = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
                                end = end - duration_diff
                                end_time_str_reconverted = end.strftime("%Y-%m-%d %H:%M")
                                step['FIN'] = end_time_str_reconverted 
                        # Update trajet
                        trajet['trajet_start_time']=trajet['steps'][0]['DEBUT']   
                        trajet['trajet_start_time']=trajet['steps'][-1]['FIN']

                    else:
                        continue  
        
        new_planning=trajet_steps_update(new_planning)
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

