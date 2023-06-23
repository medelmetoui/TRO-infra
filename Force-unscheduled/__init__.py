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

    new_planning = json.loads(planning)
    new_demande = json.loads(demande)

    # ## First update planning
    for trajet in new_planning:
        for idx,step in enumerate(trajet['steps']):
            if step['Magasin'] in new_demande['LIB_MAGASIN']:
                if step['etat'] == "Dechargement":
                    quantity_to_add = float(new_demande['DEMANDE'])
                    current_qte = float(step['quantity'])
                    current_qte = current_qte + quantity_to_add
                    step['quantity'] = str(current_qte)
                    
    
    return func.HttpResponse(json.dumps(new_planning), mimetype="application/json")

                

    
  