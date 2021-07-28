#!/usr/bin/env python3

import json
import re
from cognite.client import CogniteClient
import requests
import ssl
from urllib import request, parse
from datetime import datetime
import  pprint 
import logging
import os, sys
import time

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)

projects = {
    "ITG-1692": {
        "id": "a9f70583f-fe10-4a93-b42e-09c6c9ded724",
        "dev": True
    },
    "nancy-test": {
        "id": "aaa633d35-883f-4f89-8910-c99734c93af7",
        "dev": False
    },
    "nancy-1": {
        "id": "a4d37ff40-ea3b-4473-9326-db91e58bf23c",
        "dev": False
    },
    "int-test": {
        "id": "a8a721bda-ce34-43e8-bc08-6a1d129897df",
        "dev": True
    },
    "Dev": {
        "id": "ae377cfa2-2db4-4472-a14b-c20e5ebdd80f",
        "dev": True
    },
    "nancy-cognite": {
        "id": "a4d895117-a1f5-42da-a2ee-8b0e62899c8e",
        "dev": False
    },
    "noproject": {
        "id": "000",
        "dev": True
    }
}

def IngestRaw(cdf_client, database, table, data):
    try:
        res = cdf_client.raw.databases.create(database)
    except Exception as e:
        if not (str(e).startswith("Databases with the following names already exists")):
            raise

    try:
       res = cdf_client.raw.tables.create(database, table)
    except Exception as e:
        if not (str(e).startswith("Tables already created")):
             raise
 
    cdf_client.raw.rows.insert(database, table, json.loads(data))
   

#def map_dry_weight(dryWeight: str) -> dict:
#  result = {}
#  result["value"] = float(re.search(r'\d+', dryWeight).group()) #Searches for all the digits
#  result["unit"] = re.search(r'[a-zA-Z]+', dryWeight).group() #Searches for all the string
#  return result

def FetchRaw(cdf_client, itg_sample_raw_database):
    try:
        for table in cdf_client.raw.tables.list(itg_sample_raw_database):
            logging.info("%s" % table.name)
            for row in cdf_client.raw.rows.list(itg_sample_raw_database, table.name):
                print("\t key={} row={}".format(row.key, row.columns))
#                    for column in row.columns:
#                        print("\t %s" % (column))
    except Exception as e:
        logging.error(e)

def map_dry_weight(dryWeight: str) -> dict:
  result = {}
  result["value"] = float(re.search(r'\d+', dryWeight).group()) #Searches for all the digits
  result["unit"] = re.search(r'[a-zA-Z]+', dryWeight).group() #Searches for all the string
  return result

def Ingest2Itg(items, data_type, project, api_key):
    if projects[project]["dev"] :
        hostname = "http://localhost:3001"
    else:
        hostname = "https://itg.cognite.ai"

    url = "%s/api/v2/projects/%s/json/%s" % (hostname, projects[project]["id"], data_type)
    headers = {
        'Authorization': "Bearer {}".format(api_key), 
        'Content-Type': 'application/json'
    }
    print(headers)

    logging.info(url)
    pp = pprint.PrettyPrinter(indent=4)
    logging.info("Items:")
    pp.pprint(items)

    try:
        res = requests.post(url, headers=headers, json=items, verify=False)
        logging.info("Http response code %d, response:" % (res.status_code))
        pp.pprint(res.json())
        if 'message' in res.json():
            logging.error("message: %s" % res.json()["message"])
        if 'errors' in res.json():
            for error in res.json()['errors']:
                logging.error("Error:")
                for k,v in error.items():
                    logging.error("     %s: %s" % (k, str(v)))

                
    except Exception as e:
        logging.error(e)

def TransformIngest(cdf_client, itg_sample_raw_database, api_key):
    items = [item.columns for item in cdf_client.raw.rows.list(itg_sample_raw_database, "Equipment0")]
    print("--- Equipment")
    for item in items:
        print(item)

    print("--- decapitalize")
    items = [{key[:1].lower() + key[1:] : value for key,value in item.items()} for item in items]
    for item in items:
        print(item)

    print("--- preserve necessary fields")
    keys_needed = ['id', 'description', 'isOperational', 'parent', 'dryWeight', 'area']
    items = [{key:value for key,value in item.items() if key in keys_needed} for item in items]
    for item in items:
        print(item)

    print("--- put dry weight to object")
    for item in items:
        item['dryWeight'] = map_dry_weight(item['dryWeight'])
    for item in items:
        print(item)

    print("--- put parent and area to object")
    for item in items:
        item['parent'] = {'id': item['parent']}
        item['area'] = {'id': item['area']}
    for item in items:
        print(item)

    Ingest2Itg(items, "Equipment", "ITG-1692", api_key)

#    print("--- Area")
#    items = [item.columns for item in cdf_client.raw.rows.list(itg_sample_raw_database, "Area6")]
#    for item in items:
#        print(item)

#    print("--- put equipments to object")
#    for item in items:
#        #item['equipments'] = {'id': item['equipments']}
#        del item['equipments']
#    for item in items:
#        print(item)
#
#    Ingest2Itg(items, "Area", "ITG-1692")

def IngestAreas(cdf_client, initArea, finalArea):
    database = "ItgSampleDataPop"
    table = "Area"
    for id in range(initArea, finalArea):
        area_json = """
        {
            "r%d":{
                "id":"a%d",
                "name":"Area%d"
            }
         }
         """ % (id, id, id)
        IngestRaw(cdf_client, database, table, area_json)
        logging.info("Successfully ingested area %d" % id)
        time.sleep(1)

if __name__=="__main__":
    api_key = os.environ['API_KEY'] 
    tenant = os.environ['TENANT']
 
    client_name = "ingestion-pipeline"
    cdf_client = CogniteClient(api_key=api_key, client_name=client_name, project=tenant)
 
    #IngestRaw(cdf_client, itg_sample_raw_database)
 
    #FetchRaw(cdf_client, itg_sample_raw_database)
 
    #TransformIngest(cdf_client, itg_sample_raw_database, api_key)

    IngestAreas(cdf_client, 3, 20)
