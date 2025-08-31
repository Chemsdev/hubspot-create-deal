from tools            import *
from matching_company import *

import json, re


def lambda_handler(event, context):
    """
    Lambda pour créer la transaction dans Hubspot depuis le dernier JSON DEAL
    et mettre à jour le log existant correspondant au PDF.
    """

    BUCKET = "hubspot-tickets-pdf"
    FOLDER = "DEAL_JSON"

    # ----------------------------------------------------------->
    # (1) Connexion AWS
    aws_conn = connexion_aws()
    if aws_conn["status"] != "success":
        return {"statusCode": 500, "body": json.dumps({"error": "Connexion AWS échouée"})}
    s3_client = aws_conn["client"]
    # ----------------------------------------------------------->

    try:
        
        # ----------------------------------------------------------->
        # (2) Récupérer le dernier JSON DEAL
        llm_data, file_name = get_last_json(s3_client, bucket=BUCKET, prefix=FOLDER)
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (3) Extraire le nom du fichier PDF
        match = re.search(r"\[(.*?)\]", file_name)
        if match:
            base_name = match.group(1)
            print(f"📄 Dernier fichier DEAL trouvé : {base_name}")
        else:
            raise ValueError("Impossible d'extraire le nom du PDF depuis le fichier JSON")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (4) Récupération du fichier json des loggings du fichier PDF correspondant.
        log_key     = f"LOGS/log_[{base_name}].json"
        log_obj     = s3_client.get_object(Bucket=BUCKET, Key=log_key)
        log_content = log_obj['Body'].read().decode('utf-8')
        log_data    = json.loads(log_content)
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (5) Matching : Récupération des informations de l'entreprise sur Hubspot.
        infos_entreprise_pdf = llm_data["entreprise"]
        matching_list = find_hubspot_company_ids([infos_entreprise_pdf], min_score=75)

        if not matching_list:
            raise ValueError("Aucun résultat de matching Hubspot retourné")

        # Récupération du résultat.
        matching = matching_list[0]  
        # ----------------------------------------------------------->

        if matching.get("match") == "found":
            
            # Infos matching à stocker pour les loggings.
            matching_info = {
                "match"        : matching.get("match"),
                "hs_object_id" : matching.get("hs_object_id"),
                "matched_name" : matching.get("matched_name"),
                "score"        : matching.get("score"),
                "method"       : matching.get("method"),
                "client_naali" : matching.get("client_naali")
            }

            # Création du JSON pour la transaction Hubspot
            commande = {
                "nom"             : llm_data.get("entreprise", {}).get("nom"),
                "id_hubspot"      : matching.get("hs_object_id"),
                "is_naali_client" : matching.get("client_naali"),
                "total_price"     : llm_data.get("total"),
                "products"        : llm_data["produits"]
            }

            # ----------------------------------------------------------->
            # (6) Création de la commande dans Hubspot
            deal_id = create_transaction_with_line_product(commande=commande)
            deal_id=12345
            print(f"✅ Transaction créée dans Hubspot pour le PDF {base_name}")
            # ----------------------------------------------------------->
        
            # ----------------------------------------------------------->
            # (7) Mise à jour du log
            log_data["workflow"]["DEAL"]["status"]  = "Success"
            log_data["workflow"]["DEAL"]["details"] = "Created in Hubspot"

            log_data["workflow"]["DEAL"]["transaction"]["dealname"] = "TEST-" + commande["nom"]
            log_data["workflow"]["DEAL"]["transaction"]["id_deal"]  = deal_id

            log_data["workflow"]["DEAL"]["matching_company"] = matching_info
            # ----------------------------------------------------------->

            # ----------------------------------------------------------->
            # (8) Sauvegarde du log mis à jour dans S3
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json"
            )
            print(f"✅ Log mis à jour dans S3 ({log_key})")
            # ----------------------------------------------------------->

            return {
                "statusCode": 200,
                "body": json.dumps({"status": "ok", "message": f"Transaction créée pour le fichier PDF {base_name}"})
            }

        else:
            
            # Aucun matching.
            log_data["workflow"]["DEAL"]["status"]  = "Failed"
            log_data["workflow"]["DEAL"]["details"] = "Aucun matching"
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json"
            )
            return {
                "statusCode": 404,
                "body": json.dumps({"status": "failed", "message": "Aucun matching Hubspot trouvé"})
            }

    except Exception as e:
        
        # ----------------------------------------------------------->
        # (9) Gestion d'erreur
        print(f"❌ Erreur inattendue : {e}")
        if 'log_data' in locals() and 'log_key' in locals():
            log_data["workflow"]["DEAL"]["status"] = "Failed"
            log_data["workflow"]["DEAL"]["details"] = str(e)
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json"
            )
            print(f"⚠️ Log mis à jour avec l'erreur ({log_key})")

        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }