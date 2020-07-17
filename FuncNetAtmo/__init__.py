import datetime
import logging
import requests
import os 
import json
import uuid

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions

#Get username and secret from Azure Function
netatmousername = os.environ['NETATMO_USERNAME']
netatmopasswd = os.environ['NETATMO_PWD']
netatmoclientid = os.environ['CLIENT_ID']
netatmosecret = os.environ['CLIENT_SECRET']
storagesasurl = os.environ['STORAGE_SAS']
storagecontainer = os.environ['CONTAINER']
cosmosuri = os.environ['COSMOS_ACCOUNT_URI']
cosmoskey = os.environ['COSMOS_KEY']

print("Netatmo Azure Function v0.1")

def GetNetatmoData():
    payload = {'grant_type': "password",
       'username': netatmousername,
       'password': netatmopasswd,
       'client_id':netatmoclientid,
       'client_secret': netatmosecret,
       'scope': 'read_station'}

    try:   #Get Access Token
        response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
        response.raise_for_status()
        access_token=response.json()["access_token"]
        refresh_token=response.json()["refresh_token"]
        scope=response.json()["scope"]
        params = { 'access_token': access_token }
    except requests.exceptions.HTTPError as error:
        print(error.response.status_code, error.response.text)


    try:    #Get Json Data
        response = requests.post("https://api.netatmo.com/api/getstationsdata", params=params)
        response.raise_for_status()
        data = response.json()["body"]
        #data2= json.dumps(data)
        #print(data2)
        return data
    except requests.exceptions.HTTPError as error:
        print(error.response.status_code, error.response.text)

def UploadBlob(guid, jsondata):

    try: #Create File in temp folder
        filename = guid + ".json"
        f = open("/tmp/"+filename, "w")
        f.write(json.dumps(jsondata))
        f.close()
        logging.info("File saved:" + filename)
    except StorageErrorException:
        print("Error: Cant write file to /tmp/" + filename)

    try: #upload to storage
        sas_url = storagesasurl
        blob_service_client = BlobServiceClient.from_connection_string(sas_url)
        container_client = blob_service_client.get_container_client(storagecontainer)
        blob_client = container_client.get_blob_client("netatmo/"+filename)
        with open("/tmp/" + filename, "rb") as data:
            blob_client.upload_blob(data)
        logging.info("File uploaded:" + filename)
    except:
        print("Error: Cant upload the data to netatmo/" + filename)

def UploadCosmos(guid, jsondata):

    client = CosmosClient(cosmosuri, credential = cosmoskey)
    database_name = 'cosmosnetatmo'
    container_name = 'mesures'

    try: # create database if not exists
        database = client.create_database(database_name)
    except exceptions.CosmosResourceExistsError:
        database = client.get_database_client(database_name)

    try: # create collection if not exists
        container = database.create_container(id=container_name, partition_key=PartitionKey(path="/id"))
    except exceptions.CosmosResourceExistsError:
        container = database.get_container_client(container_name)
    except exceptions.CosmosHttpResponseError:
        raise

    #Upsert after adding a guid as key to the document
    jsondata["id"] = guid
    container.upsert_item(jsondata)
    #container.create_item(body=jsondata)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    myguid = str(uuid.uuid4())
    netatmodata = GetNetatmoData()
    UploadBlob(myguid, netatmodata)
    UploadCosmos(myguid, netatmodata )

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

#if __name__ == "__main__":
#    main()
