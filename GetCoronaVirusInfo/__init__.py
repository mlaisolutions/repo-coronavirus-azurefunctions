import logging

import azure.functions as func
from azure.storage.blob import BlockBlobService
import pandas as pd
import tables
#import geopandas as gpd
import geojson
import json


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
        if name=="all":
            df1 = get_data_frame('confirmed')
            df2 = get_data_frame('recovered')
            df3 = get_data_frame('deaths')
            df = pd.concat([df1,df2,df3])
        else:
            df = get_data_frame(name)
            df['CaseType']=name
        cols = ['Province/State','Country/Region','Lat','Long','LatestDate','LatestCount','CaseType']
        geojson = df_to_geojson(df, cols,'Lat','Long')
        geojson_str = json.dumps(geojson, indent=2)
        return func.HttpResponse(geojson_str)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
def df_to_geojson(df, properties, lat='latitude', lon='longitude'):
    geojson = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'Point',
                               'coordinates':[]}}
        feature['geometry']['coordinates'] = [row[lon],row[lat]]
        for prop in properties:
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)
    return geojson
def get_data_frame(casetype):
    STORAGEACCOUNTNAME= 'storagelabcoronavirus'
    STORAGEACCOUNTKEY= 'OcSGt1fCikgKjViIJTa6D0EcPLl2mK0YHVbxH25B7D4YV9VdwR/RYJ91sJqLbnh9janaMTOt6+fqChY+gsFFxQ=='
    LOCALFILENAME= 'local_file_name'
    CONTAINERNAME= 'data'
    if casetype=="confirmed":
        BLOBNAME = "time_series_covid_19_confirmed.csv"
    elif casetype=="recovered":
        BLOBNAME = "time_series_covid_19_recovered.csv"
    elif casetype=="deaths":
        BLOBNAME = "time_series_covid_19_deaths.csv"
    else:
        BLOBNAME= 'time_series_covid_19_confirmed.csv'


    blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
    blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
    #message = "File "+ BLOBNAME + "downloaded." 
    cases_df = pd.read_csv(LOCALFILENAME)
    cases_df['Lat'] = cases_df['Lat'].astype(float)
    cases_df['Long'] = cases_df['Long'].astype(float)
    cases_df['LatestDate'] = cases_df.columns[-1]
    cases_df.rename(columns = {cases_df.columns[-2]:'LatestCount'}, inplace = True) 
    cases_df['CaseType'] = casetype
    #confirmed = gpd.GeoDataFrame(confirmed_df, geometry=gpd.points_from_xy(confirmed_df.Long, confirmed_df.Lat))
    cases_df = cases_df.loc[:, ['Province/State','Country/Region','Lat','Long','LatestDate','LatestCount','CaseType']]
    cases_df = cases_df.dropna()
    #'split’ : dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
    #'records’ : list like [{column -> value}, … , {column -> value}]
    #'index’ : dict like {index -> {column -> value}}
    #'columns’ : dict like {column -> {index -> value}}
    #‘values’ : just the values array
    #‘table’ : dict like {‘schema’: {schema}, ‘data’: {data}}
    #strGeoJson = cases_df.to_json(orient="records")
    #return func.HttpResponse(strGeoJson)
    return cases_df
