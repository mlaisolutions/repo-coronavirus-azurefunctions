import logging

import azure.functions as func
from azure.storage.blob import BlockBlobService
import pandas as pd
import tables
#import geopandas as gpd

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
        confirmed_df = pd.read_csv(LOCALFILENAME)
        confirmed_df['Lat'] = confirmed_df['Lat'].astype(float)
        confirmed_df['Long'] = confirmed_df['Long'].astype(float)
        #confirmed = gpd.GeoDataFrame(confirmed_df, geometry=gpd.points_from_xy(confirmed_df.Long, confirmed_df.Lat))
        confirmed_df = confirmed_df.loc[:, ['Province/State','Country/Region','Lat','Long',confirmed_df.columns[-1]]]
        strGeoJson = confirmed_df.to_json()
        #return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
        return func.HttpResponse(strGeoJson)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
