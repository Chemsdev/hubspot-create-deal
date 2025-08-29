from hubspot.crm.deals import SimplePublicObjectInputForCreate
import os
import requests
from datetime import datetime  
import hubspot
from dotenv import load_dotenv
import json


load_dotenv()
headers = {
    "Authorization": f"Bearer {os.environ.get('ACCESS_TOKEN_HUBSPOT')}",
    "Content-Type": "application/json"
}




ACCESS_TOKEN_HUBSPOT = os.getenv("HUBSPOT_API_KEY")


# ------------------------------------------------------------------------>

# Fonction permettent de récupérer le dernier fichier JSON du dossier.
def get_last_json(s3_client, bucket: str, prefix: str) -> dict:
    """
    Récupère le fichier JSON le plus récent dans le dossier S3 et le retourne en dictionnaire Python.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        raise FileNotFoundError(f"Aucun fichier trouvé dans {prefix}")

    # Trier les fichiers par date de modification décroissante
    sorted_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)

    # Récupérer le dernier fichier (le plus récent)
    last_json_key = sorted_files[0]["Key"]

    # Télécharger le contenu du fichier JSON
    obj = s3_client.get_object(Bucket=bucket, Key=last_json_key)
    json_content = obj["Body"].read().decode("utf-8")

    # Convertir le JSON en dictionnaire Python
    data = json.loads(json_content)

    return data

# ------------------------------------------------------------------------>

# Fonction permettent de récupérer la date actuelle au format ISO.
def get_current_iso8601_date():
    now = datetime.utcnow()  
    iso8601_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return iso8601_date

# ------------------------------------------------------------------------>

# Fonction permettent de créer les objets transactions & associations : IMPLANTATION / REASSORT
def get_object_hubspot(hubspot_id_company:int, deal_name:str, amount:float, client_Naali:bool):
    
    if not client_Naali:
        
        # ----------------------------------------------------------->
        IMPLANTATION = [
            {
                # Transaction.
                "amount"    :  amount,
                "closedate" :  get_current_iso8601_date(),
                "dealname"  :  deal_name,
                "pipeline"  : "default",
                "dealstage" : "closedwon"
            },
            [{
                # Associations.
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 5
                    }
                ],
                "to": {
                    "id": hubspot_id_company
                }
            }]
        ]
        
        # Récupération des objets transactions & associations.
        transaction  = IMPLANTATION[0]
        associations = IMPLANTATION[1]
        return transaction, associations
        # ----------------------------------------------------------->
    

    if client_Naali :
        
        # ----------------------------------------------------------->
        REASSORT = [
            
            {
                # Transaction.
                "amount"   :  amount,
                "closedate":  get_current_iso8601_date(),
                "dealname" :  deal_name,
                "pipeline" : "1543644371",
                "dealstage": "2110945486"
            },
            [{
                # Associations.
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED", 
                        "associationTypeId"  : 341
                    
                    }
                ],
                "to": {
                    "id": hubspot_id_company
                }
            }]
        ]

        # Récupération des objets transactions & associations.
        transaction  = REASSORT[0]
        associations = REASSORT[1]
        return transaction, associations
        # ----------------------------------------------------------->

# ------------------------------------------------------------------------>

# Association des lignes produits à une transaction.
def create_line_item_and_associate_to_deal(product:dict, deal_id:int):
    url = "https://api.hubapi.com/crm/v3/objects/line_items"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN_HUBSPOT}",
        "Content-Type": "application/json"
    }
    line_item_data = {
        "properties": {
            "name"          : product['name'],
            "price"         : float(product['price']),
            "quantity"      : float(product['quantity']),
            "hs_product_id" : str(product['hs_product_id'])
        },
        "associations": [
            {
                "to": {
                    "id": str(deal_id)
                },
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 20
                    }
                ]
            }
        ]
    }
    response = requests.post(url, headers=headers, json=line_item_data)
    response.raise_for_status()
    line_item_id = response.json()['id']
    print(f"Ligne produit créée et associée avec ID: {line_item_id}")
    return line_item_id

# ------------------------------------------------------------------------>

# Fonction permettent de créer la transaction avec les lignes produits.
def create_transaction_with_line_product(commande:dict, DEV=True):
    
 
    
    # ----------------------------------------->
    # Création des dictionnaires.
    transaction, associations = get_object_hubspot(
            hubspot_id_company  = commande["id_hubspot"], 
            deal_name           = "TEST" + commande["nom"], 
            
            # A voir.
            client_Naali        = commande["is_naali_client"],
            
            # A voir.
            amount              = commande["total_price"]
    )
        
    # Création de l'objet Transaction avec le bon pipeline.
    simple_public_object_input = SimplePublicObjectInputForCreate(associations=associations, properties=transaction)
    # ----------------------------------------->


    if not DEV: 
        
        # --------------------------->
        # Création de la transaction & récupération de son ID.
        client = hubspot(access_token=ACCESS_TOKEN_HUBSPOT)
        api_response = client.crm.deals.basic_api.create(simple_public_object_input_for_create=simple_public_object_input)
        deal_id = api_response.id
        
        # Association des lignes produits à la transaction.
        for i in commande["products"]:
            create_line_item_and_associate_to_deal(product=i, deal_id=deal_id)
        # --------------------------->
            
# ------------------------------------------------------------------------>