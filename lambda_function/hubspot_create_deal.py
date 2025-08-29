# main.py

import os
import boto3


from tools import *

# ------------------------>
AWS_CONNEXION_CHEMS = [
    "ACCESS_KEY_ID_CHEMS",   
    "SECRET_ACCESS_KEY_CHEMS",
    "REGION_CHEMS"      
]
# ------------------------>


# ------------------------>
# Fonction permettent de se connecter à AWS. 
def connexion_aws(liste_connexion=AWS_CONNEXION_CHEMS):
    try:
        load_dotenv()
        s3_client = boto3.client(
            's3',
            aws_access_key_id     = os.environ.get(liste_connexion[0]),
            aws_secret_access_key = os.environ.get(liste_connexion[1]),
            region_name           = os.environ.get(liste_connexion[2])
        )
        
        message = f"Connexion AWS réussie (région : {os.environ.get(liste_connexion[2])})."
        print(message)
        
        return {
            "status"  : "success",
            "message" : message,
            "client"  : s3_client
        }

    except Exception as e:
        error_message = f"Échec de la connexion AWS : {e}"
        print(error_message)

        return {
            "status"  : "error",
            "message" : error_message,
            "client"  : None
        }
# ------------------------>


def lambda_handler(event, context):
    
    s3_client = connexion_aws(liste_connexion=AWS_CONNEXION_CHEMS)
    BUCKET    = "hubspot-tickets-pdf"
    FOLDER    = "DEAL_JSON"
    
    try:
        
        # ------------------------------------------------------------------------------------>
        # 1. Connexion AWS.
        aws_conn = connexion_aws()
        if aws_conn["status"] != "success":
            raise RuntimeError("Connexion AWS échouée")
        s3_client = aws_conn["client"]
        
        # 2. Récupération du dernier JSON contenant les informations de la commande.
        commande=get_last_json(s3_client, bucket=BUCKET, prefix=FOLDER)
        
        # 3. Création de la commande dans Hubspot.
        create_transaction_with_line_product(commande=commande)
        # ------------------------------------------------------------------------------------>

        return {"status": "ok", "message": ""}

    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        return {"status": "error", "message": str(e)}
