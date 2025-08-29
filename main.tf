provider "aws" {
  region = "eu-central-1"
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.45.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7.1"
    }
  }

  required_version = ">= 1.12.0"
}

# Récupération dynamique des dernières versions des layers existants
data "aws_lambda_layer_version" "python-dotenv" {
  layer_name = "python-dotenv"
}

data "aws_lambda_layer_version" "requests" {
  layer_name = "requests"
}

data "aws_lambda_layer_version" "hubspot-api-client" {
  layer_name = "hubspot-api-client"
}

# Création du fichier ZIP de la fonction Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_function"
  output_path = "${path.module}/lambda_function.zip"
}

# Déploiement de la fonction Lambda avec layers dynamiques
resource "aws_lambda_function" "hubspot_create_deal" {
  function_name = "hubspot-create-deal"
  handler       = "hubspot_create_deal.lambda_handler"
  runtime       = "python3.9"
  role          = "arn:aws:iam::975515885951:role/lambda"

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  timeout = 900 

  # Variables d'environnement.
  environment {
    variables = {
      ACCESS_TOKEN_HUBSPOT = var.ACCESS_TOKEN_HUBSPOT
    }
  }

  # Les layers.
  layers = [
    data.aws_lambda_layer_version.requests.arn,
    data.aws_lambda_layer_version.python-dotenv.arn,
    data.aws_lambda_layer_version.hubspot-api-client.arn

  ]
}