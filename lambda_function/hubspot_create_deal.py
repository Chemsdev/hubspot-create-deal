from tools             import *
from matching_company  import *
from matching_products import *

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
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Connexion AWS échouée"})
        }
    s3_client = aws_conn["client"]
    # ----------------------------------------------------------->

    try:
        
        # ----------------------------------------------------------->
        # (2) Récupérer le dernier JSON DEAL.
        llm_data, file_name = get_last_json(
            s3_client, bucket=BUCKET, prefix=FOLDER
        )
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (3) Extraire le nom du fichier PDF.
        match = re.search(r"\[(.*?)\]", file_name)
        if not match:
            raise ValueError("Impossible d'extraire le nom du PDF depuis le fichier JSON")
        base_name = match.group(1)
        print(f"📄 Dernier fichier DEAL trouvé : {base_name}")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (4) Charger le log JSON correspondant.
        log_key     = f"LOGS/log_[{base_name}].json"
        log_obj     = s3_client.get_object(Bucket=BUCKET, Key=log_key)
        log_content = log_obj["Body"].read().decode("utf-8")
        log_data    = json.loads(log_content)
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (5) Matching Entreprise (depuis PDF -> Hubspot).
        infos_entreprise_pdf = llm_data["entreprise"]
        matching_company     = find_hubspot_company_ids(
            [infos_entreprise_pdf], min_score=75
        )
        # ----------------------------------------------------------->

        # Si l'entreprise n'a pas été retrouvé, un enregistre le logging avec l'erreur.
        if matching_company.get("match") != "found":
            log_data["workflow"]["DEAL"]["status"]  = "Failed"
            log_data["workflow"]["DEAL"]["details"] = "Aucun matching entreprise trouvé"
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json",
            )
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "status": "failed",
                    "message": "Aucun matching Hubspot trouvé pour l'entreprise"
                }),
            }
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (6) Matching Produits (depuis PDF -> Hubspot).
        infos_produits_pdf = llm_data["produits"]
        matching_products  = match_products_preserve_shape(
            infos_produits_pdf, min_score=78, force_refresh=True
        )

        # Liste permettent d'enregistrer les produits non retrouvés sur Hubspot.
        missing_matching_products = []
        for i in matching_products:
            
            # Récupération du nom du produit en input.
            product_name = i["input"]["nom_produit"]

            # Si le produit n'as pas été retrouvé sur Hubspot, on l'ajoute à la liste.
            if i["match"] == "not_found":
                missing_matching_products.append(product_name)

            # Enregistrement du résultat du matching du produit dans le logging.
            log_data["workflow"]["DEAL"]["matching_products"][product_name] = {
                "match"         : i["match"],
                "hs_object_id"  : i["hs_object_id"],
                "matched_name"  : i["matched_name"],
                "matched_price" : i["matched_price"],
                "score"         : i["score"],
                "method"        : i["method"],
            }

        # Enregistrement dans le logging les produits non retrouvées sur Hubspot.
        if missing_matching_products:
            log_data["workflow"]["DEAL"]["status"]  = "Failed"
            log_data["workflow"]["DEAL"]["details"] = (f"Matching non retrouvé pour les produits : {missing_matching_products}")
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json",
            )
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "status": "failed",
                    "message": f"Matching non retrouvé pour les produits : {missing_matching_products}"
                }),
            }
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (7) Préparation des lignes produits en y associant leurs ID hubspot.
        ligne_produits = []
        for i in matching_products:
            produit = {
                "name"         : i["matched_name"],
                "price"        : i["matched_price"],
                "quantity"     : i["input"]["quantite"],
                "hs_product_id": i["hs_object_id"],
            }
            ligne_produits.append(produit)
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (8) Création de la transaction Hubspot.
        commande = {
            "nom"            : llm_data.get("entreprise", {}).get("nom"),
            "id_hubspot"     : matching_company.get("hs_object_id"),
            "is_naali_client": matching_company.get("client_naali"),
            "total_price"    : llm_data.get("total"),
            "products"       : ligne_produits,
        }

        deal_id = create_transaction_with_line_product(commande=commande)
        print(f"✅ Transaction créée dans Hubspot pour le PDF {base_name}")
        # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (9) Mise à jour du logging.
        log_data["workflow"]["DEAL"]["status"]                       = "Success"
        log_data["workflow"]["DEAL"]["details"]                      = "Created in Hubspot"
        log_data["workflow"]["DEAL"]["transaction"]["dealname"]      = "TEST-" + commande["nom"]
        log_data["workflow"]["DEAL"]["transaction"]["id_deal"]       = deal_id
        
        # Enregistrement du résultat du matching enrreprise.
        log_data["workflow"]["DEAL"]["matching_company"]             = {
            "match"        : matching_company.get("match"),
            "hs_object_id" : matching_company.get("hs_object_id"),
            "matched_name" : matching_company.get("matched_name"),
            "score"        : matching_company.get("score"),
            "method"       : matching_company.get("method"),
            "client_naali" : matching_company.get("client_naali"),
        }

        s3_client.put_object(
            Bucket=BUCKET,
            Key=log_key,
            Body=json.dumps(log_data, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )
        print(f"✅ Log mis à jour dans S3 ({log_key})")
        # ----------------------------------------------------------->

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "ok",
                "message": f"Transaction créée pour le fichier PDF {base_name}"
            }),
        }

    except Exception as e:
        # ----------------------------------------------------------->
        # (10) Gestion d'erreur.
        print(f"❌ Erreur inattendue : {e}")
        if "log_data" in locals() and "log_key" in locals():
            log_data["workflow"]["DEAL"]["status"]  = "Failed"
            log_data["workflow"]["DEAL"]["details"] = str(e)
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body=json.dumps(log_data, ensure_ascii=False, indent=2),
                ContentType="application/json",
            )
            print(f"⚠️ Log mis à jour avec l'erreur ({log_key})")

        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)}),
        }
        
