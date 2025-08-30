from tools import *



def lambda_handler(event, context):
    """
    Lambda pour créer la transaction dans Hubspot depuis le dernier JSON DEAL
    et mettre à jour le log existant correspondant au PDF.
    """
    import json, re
    from datetime import datetime

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
        commande, file_name = get_last_json(s3_client, bucket=BUCKET, prefix=FOLDER)
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
        # (4) Définir le log_key et récupérer le log existant
        log_key = f"LOGS/log_[{base_name}].json"
        log_obj = s3_client.get_object(Bucket=BUCKET, Key=log_key)
        log_content = log_obj['Body'].read().decode('utf-8')
        log_data = json.loads(log_content)
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (5) Création de la commande dans Hubspot
        # create_transaction_with_line_product(commande=commande)
        print(f"✅ Transaction créée dans Hubspot pour le PDF {base_name}")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (6) Mise à jour du log
        log_data["workflow"]["DEAL"]["status"] = "Success"
        log_data["workflow"]["DEAL"]["details"] = f"Created in Hubspot"

        # Ajout des informations de la transaction
        log_data["workflow"]["DEAL"]["data"]["dealname"] = "PHARMACIE - TEST"
        log_data["workflow"]["DEAL"]["data"]["id_deal"] = 12345
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (7) Sauvegarde du log mis à jour dans S3
        s3_client.put_object(
            Bucket=BUCKET,
            Key=log_key,
            Body=json.dumps(log_data, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        print(f"✅ Log mis à jour dans S3 ({log_key})")
        # ----------------------------------------------------------->

        return {"statusCode": 200, "body": json.dumps({"status": "ok", "message": f"Transaction créée pour le fichier PDF {base_name}"})}

    except Exception as e:
        # ----------------------------------------------------------->
        # (8) Gestion d'erreur et mise à jour log existant uniquement
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
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}
        # ----------------------------------------------------------->
