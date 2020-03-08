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
        elif name=="consolidated":
            df = get_consolidated_data()
        else:
            df = get_data_frame(name)
            df['CaseType']=name
        cols = ['Province/State','Country/Region','Lat','Long','LatestDate','LatestCount','CaseType','Confirmed','Deaths','Recovered']
        geojson = df_to_geojson(df, cols,'Lat','Long')
        geojson_str = json.dumps(geojson)
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
    #message = "File "+ BLOBNAME + "downloaded." 
    cases_df = pd.read_csv(get_storage_filehandler(casetype))
    cases_df['Lat'] = cases_df['Lat'].astype(float)
    cases_df['Long'] = cases_df['Long'].astype(float)
    cases_df['LatestDate'] = cases_df.columns[-1]
    cases_df.rename(columns = {cases_df.columns[-2]:'LatestCount'}, inplace = True) 
    cases_df['CaseType'] = casetype
    cases_df['Confirmed'] = 0
    cases_df['Deaths'] = 0
    cases_df['Recovered'] = 0


    #confirmed = gpd.GeoDataFrame(confirmed_df, geometry=gpd.points_from_xy(confirmed_df.Long, confirmed_df.Lat))
    cases_df = cases_df.loc[:, ['Province/State','Country/Region','Lat','Long','LatestDate','LatestCount','CaseType','Confirmed','Deaths','Recovered']]
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
def get_storage_filehandler(casetype):
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
    elif casetype=="consolidated":
        BLOBNAME = "2019_nCoV_data.csv"
    else:
        BLOBNAME= 'time_series_covid_19_confirmed.csv'

    blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
    blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
    return LOCALFILENAME
def get_consolidated_data():
    confirmed =  pd.read_csv(get_storage_filehandler('confirmed'))
    consolidated = pd.read_csv(get_storage_filehandler('consolidated'))
    confirmed['Lat'] = confirmed['Lat'].astype(float)
    confirmed['Long'] = confirmed['Long'].astype(float)
    confirmed["LatestDate"] = confirmed.columns[-1]
    confirmed.rename(columns = {confirmed.columns[-2]:'LatestCount'}, inplace = True) 
    confirmed['CaseType'] = "confirmed"
    confirmed = confirmed.loc[:, ['Province/State','Country/Region','Lat','Long','LatestDate','LatestCount','CaseType']]
    confirmed['CaseType'] = "confirmed"
    consolidated.rename(columns = {"Last Update":'LatestDate'}, inplace = True) 
    consolidated["Lat"]= 0.00
    consolidated["Long"] = 0.00
    merged = consolidated.merge(confirmed, left_on='Province/State',right_on='Province/State', suffixes=('_left', '_right'),how='left')
    merged.rename(columns = {"Lat_right":'Lat'}, inplace = True)
    merged.rename(columns = {"Long_right":'Long'}, inplace = True)
    merged.rename(columns = {"LatestDate_right":'LatestDate'}, inplace = True)
    merged.rename(columns = {"Country/Region":'Country/RegionDeleted'}, inplace = True)
    merged.rename(columns = {"Country":'Country/Region'}, inplace = True)
    #merged.rename(columns = {"lat_right":'Lat',"long_right":'Long',"latestdate_left":'LatestDate'}, inplace = True)
    merged["LatestCount"] = 0
    merged['Lat'] = merged['Lat'].astype(float)
    merged['Long'] = merged['Long'].astype(float)

    merged['Confirmed'] = merged['Confirmed'].astype(int)
    merged['Recovered'] = merged['Recovered'].astype(int)
    merged['Deaths'] = merged['Deaths'].astype(int)
    merged = merged.loc[:, ['Province/State','Country/Region','Lat','Long','LatestDate','LatestCount','CaseType','Confirmed','Deaths','Recovered']]
    merged.dropna(axis=0,how='any',inplace=True)
    save_to_storage(merged,"2019_nCoV_data_merged.csv")
    return merged
def save_to_storage(df,filename):
    output = df.to_csv (index_label="idx", encoding = "utf-8")
    STORAGEACCOUNTNAME= 'storagelabcoronavirus'
    STORAGEACCOUNTKEY= 'OcSGt1fCikgKjViIJTa6D0EcPLl2mK0YHVbxH25B7D4YV9VdwR/RYJ91sJqLbnh9janaMTOt6+fqChY+gsFFxQ=='
    LOCALFILENAME= 'local_file_name'
    CONTAINERNAME= 'data'

    blobService = BlockBlobService(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY)
    blobService.create_blob_from_text(CONTAINERNAME, filename, output)
