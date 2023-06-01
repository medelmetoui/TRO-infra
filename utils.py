


def transform_format(input_data):
    """
    This function transforms JSON to CSV ready data
    """
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