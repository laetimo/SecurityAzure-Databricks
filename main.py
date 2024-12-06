import os
import requests
from azure.core.credentials import TokenCredential
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, BlobClient
from datetime import datetime, timedelta, timezone

def load_param_env():
	load_dotenv()
	return {'tenant_id' : os.getenv("TENANT_ID"),
			'client_id_secondary' : os.getenv("SP_ID_SECONDARY"),
			'client_secret_secondary' : os.getenv("SP_SECONDARY_PASSWORD"),
			'client_id_principal'    : os.getenv("SP_ID_PRINCIPAL"),
			'account_name' : os.getenv("STORAGE_ACCOUNT_NAME"),
			'container_name' : os.getenv("CONTAINER_NAME"),
			'blob_name_csv' : os.getenv("BLOB_NAME_CSV"),
			'blob_name_parquet': os.getenv("BLOB_NAME_PARQUET"),
			'keyvault_url' : os.getenv("KEYVAULT_URL"),
			'secret_name' : os.getenv("SECRET_NAME")
	}

def auth(param, id, pwd):
	return ClientSecretCredential(param['tenant_id'], id, pwd)

def get_secret_client(key_vault_url, credential):
	secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
	secret = secret_client.get_secret(param['secret_name'])
	print(secret.value)# Récupérer la clé du Key Vault
	return secret.value
	
def get_blob_service_client(param, credential):
	return BlobServiceClient(account_url=f"https://{param['account_name']}.blob.core.windows.net",
											credential=credential)

def gen_user_delegation_key(blob_service_client):
	return blob_service_client.get_user_delegation_key(
			key_start_time=datetime.now(timezone.utc),
			key_expiry_time=datetime.now(timezone.utc) + timedelta(days=1)
			)
	
def create_sas_token(param, user_delegation_key, blob_name):
	return generate_blob_sas(
			param['account_name'],
			param['container_name'],
			blob_name=blob_name,
			user_delegation_key=user_delegation_key,
			permission=BlobSasPermissions(read=True, write=True, add=True),
			expiry=datetime.now() + timedelta(hours=1)
	)

def gen_blob_client(param, blob_name, sas_token):
	account_url=f"https://{param['account_name']}.blob.core.windows.net/"
	return BlobClient(account_url,
					  param['container_name'],
					  blob_name=blob_name,
					  TokenCredential=sas_token
					  )

def ingest(param, blob_name, sas_token, url):
	response = requests.get(url)
	blob_url = f"https://{param['account_name']}.blob.core.windows.net/{param['container_name']}/{blob_name}?{sas_token}"
	
	blob_client = BlobClient.from_blob_url(blob_url)  # Créer un BlobClient
	blob_client.upload_blob(response.content, overwrite=True)  # Charger le fichier dans le conteneur

def ingest_all_parquet(container_client, blob_client):
	base_url = "https://huggingface.co/datasets/Marqo/amazon-products-eval/tree/main/data/data-00"
	
	for i in range(106):   # Loop to download files from 000 to 105
		file_number = f"{i:03d}"
		file_url = f"{base_url}{file_number}-of-00105.parquet"
		
		response = requests.get(file_url)
		
		blob_client = container_client.get_blob_client(f"data00{file_number}.parquet")
		blob_client.upload_blob(response.content, overwrite=True)


############################################
csv_url = "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/visualisations/reviews.csv"
parquet_url = "https://huggingface.co/datasets/Marqo/amazon-products-eval/blob/main/data/data-00000-of-00105.parquet"

param = load_param_env()
credential_secondary = auth(param, param['client_id_secondary'], param['client_secret_secondary'])
client_secret_pr = get_secret_client(param['keyvault_url'], credential_secondary)
credential_primary = auth(param, param['client_id_principal'], client_secret_pr)
#########

blob_service_client = get_blob_service_client(param, credential_primary)
user_delegation_key = gen_user_delegation_key(blob_service_client)
container_client = blob_service_client.get_container_client(param['container_name'])

# sas_token_csv = create_sas_token(param, user_delegation_key, param['blob_name_csv'])

sas_token_parquet = create_sas_token(param, user_delegation_key, param['blob_name_parquet'])
blob_client_parquet = gen_blob_client(param, param['blob_name_parquet'], sas_token_parquet)


# Télécharger un seul fichier
# ingest(param, param['blob_name_csv'], sas_token_csv, csv_url)
# ingest(param, param['blob_name_parquet'], sas_token_parquet, parquet_url)

# Télécharger plusieurs fichiers
ingest_all_parquet(container_client, blob_client_parquet)