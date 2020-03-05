import logging

import azure.functions as func
from azure.storage.blob import BlockBlobService
import pandas as pd
import tables

STORAGEACCOUNTNAME= 'storagelabcoronavirus'
STORAGEACCOUNTKEY= 'OcSGt1fCikgKjViIJTa6D0EcPLl2mK0YHVbxH25B7D4YV9VdwR/RYJ91sJqLbnh9janaMTOt6+fqChY+gsFFxQ=='
LOCALFILENAME= 'confirmed_file_name'
CONTAINERNAME= 'data'
BLOBNAME= 'time_series_covid_19_confirmed.csv'


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
        blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
        #message = "File "+ BLOBNAME + "downloaded." 
        df = pd.read_csv(LOCALFILENAME)
        dataframe_blobdata = pd.read_csv(LOCALFILENAME)
        #return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
        return func.HttpResponse(dataframe_blobdata.to_json())
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
