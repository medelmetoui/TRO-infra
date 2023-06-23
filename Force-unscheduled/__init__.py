import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function forces One unscheduled demand into the current planning
    """
    logging.info('Python HTTP trigger function processed a request.')
    body = req.get_json()

    blob_name = str(body['planning_name'])
    
    planning = json.dumps(body['planning'])
    demande = json.dumps(body['demande'])

    ## First update planning
    for trajet in planning:
        for step in trajet:
            if step['Magasin'] == demande['LIB_MAGASIN']:
                if step['etat'] == "Dechargement":
                    quantity_to_add = float(demande['DEMANDE'])
                    current_qte = float(step['quantity'])
                    current_qte = current_qte + quantity_to_add
                    step['quantity'] = str(current_qte)
    
    return func.HttpResponse(json.dumps(planning), mimetype="application/json")

                

    
  