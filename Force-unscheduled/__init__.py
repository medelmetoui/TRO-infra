import logging
import json
import azure.functions as func
from azure.storage.blob import BlobClient,generate_blob_sas,BlobSasPermissions
from datetime import datetime,timedelta

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function forces One unscheduled demand into the current planning
    """
    logging.info('Python HTTP trigger function processed a request.')
    body = req.get_json()

    blob_name = str(body['planning_name'])
    repository_name="SEC"
    sas = generate_blob_sas(
        account_name="troblobstorage",
        container_name="output",
        blob_name=repository_name+"/" +blob_name,
        account_key="YO16wuzFj6wkoK/AjMoYjEUcPXHWbkL1BmGc280AijovwovRP64DvMIY+e5i6b+m0BVJN1jdkNQ7+AStejBP9A==", 
        permission=BlobSasPermissions(read=True, write=True, create=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    
    url = "https://troblobstorage.blob.core.windows.net/output/"+repository_name+"/" + blob_name + "?" + sas    
    blob_client = BlobClient.from_blob_url(url)
    
    planning = json.dumps(body['planning'])
    demande = json.dumps(body['demande'])

    new_planning = json.loads(planning)
    new_demande = json.loads(demande)

    # ## First update planning
    for trajet in new_planning:
        for idx,step in enumerate(trajet['steps']):
            if step['Magasin'] in new_demande['LIB_MAGASIN'] or new_demande['LIB_MAGASIN'] in step['Magasin']:
                if step['etat'] == "DECHARGEMENT":
                    quantity_to_add = float(new_demande['DEMANDE'])
                    current_qte = float(step['quantity'])
                    current_qte = current_qte + quantity_to_add
                    step['quantity'] = str(current_qte)
            else:
                continue
    
    blob_client.upload_blob(json.dumps(new_planning),overwrite=True)

    #### UPDATE THE UNSCHDULED FILE 

    repository_name="SEC"

    # active planning 
    unscheduled_file_name = "XUNSCHEDULED_QUEUE.json"

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

    for demande in unscheduled_file:
        if demande == new_demande:
            unscheduled_file.remove(demande)
            blob_client.upload_blob(json.dumps(unscheduled_file),overwrite=True)
                    
    return func.HttpResponse(json.dumps(new_planning), mimetype="application/json")

                

    
  