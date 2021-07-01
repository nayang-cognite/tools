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

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

api_key = "" 

cdf_tenant = "itg-testing"     

projects = {
    "test": {
        "id" : "a31dea04b-5dbd-47b8-96af-7d46fcadff23",
        "dev": True
    },
    "nancy-test": {
        "id": "aaa633d35-883f-4f89-8910-c99734c93af7",
        "dev": False
    }
}

def IngestRaw(cdf_client, itg_sample_raw_database):

   area_json = """
   {
       "r1":{
           "id":"a1",
           ename":"Snorre",
           "equipments":"eqPump001"
       },
       "r2":{
           "id":"a2",
           "name":"Valhall",
           "equipments":"eqPump002"
       },
       "r3":{
           "id":"a3",
           "name":"Ekofisk",
           "equipments":"eqPump003"
       }
    }
   """

   equipment_json = """
   {
      "r1":{
         "id":"eqPump001",
         "description":"Pump 1",
         "isOperational":true,
         "parent":"eqPump003",
         "dryWeight":"5 kg",
         "area":"a1"
      },
      "r2":{
         "id":"eqPump002",
         "description":"Pump 2",
         "isOperational":false,
         "parent":"eqPump001",
         "dryWeight":"300 g",
         "area":"a2"
      },
      "r3":{
         "id":"eqPump003",
         "description":"Pump 3",
         "isOperational":true,
         "parent":"eqPump002",
         "dryWeight":"2 kg",
         "area":"a3"
      }
   }
   """


   res = cdf_client.raw.databases.create(itg_sample_raw_database)
   print(res)
   
   res = cdf_client.raw.tables.create(itg_sample_raw_database, "Area")
   print(res)
   
   cdf_client.raw.rows.insert(itg_sample_raw_database, "Equipment", json.loads(equipment_json))
   print(res)
   
   cdf_client.raw.rows.insert(itg_sample_raw_database, "Area", json.loads(area_json))
   
   Equipment_raw = "https://fusion.cognite.com/{}/raw/{}/Equipment".format(cdf_tenant, itg_sample_raw_database)
   Area_raw = "https://fusion.cognite.com/{}/raw/{}/Area".format(cdf_tenant, itg_sample_raw_database)
   
   print(Area_raw)
   print(Equipment_raw)

#def map_dry_weight(dryWeight: str) -> dict:
#  result = {}
#  result["value"] = float(re.search(r'\d+', dryWeight).group()) #Searches for all the digits
#  result["unit"] = re.search(r'[a-zA-Z]+', dryWeight).group() #Searches for all the string
#  return result

def FetchRaw(cdf_client, itg_sample_raw_database):
   for table in cdf_client.raw.tables.list(itg_sample_raw_database):
       print(table)
       for row in cdf_client.raw.rows.list(itg_sample_raw_database, table.name):
           print("\t key={} row={}".format(row.key, row.columns))


def map_dry_weight(dryWeight: str) -> dict:
  result = {}
  result["value"] = float(re.search(r'\d+', dryWeight).group()) #Searches for all the digits
  result["unit"] = re.search(r'[a-zA-Z]+', dryWeight).group() #Searches for all the string
  return result

def Ingest2Itg(items, data_type, project):

    if projects[project]["dev"] :
        hostname = "http://localhost:3001"
    else:
        hostname = "https://itg.cognite.ai"

    url = "%s/api/v2/projects/%s/json/%s" % (hostname, projects[project]["id"], data_type)

    headers = {
        'Authorization': f'Bearer {api_key}', 
        'token': '',
        'Content-Type': 'application/json'
    }

    logging.info(url)
    pp = pprint.PrettyPrinter(indent=4)
    logging.info("Items:")
    pp.pprint(items)

    try:
        res = requests.post(url, headers=headers, json=items, verify=False)
        logging.info("Http response code %d, response:" % (res.status_code))
        pp.pprint(res.json())
    except Exception as e:
        logging.error(e)

def TransformIngest(cdf_client, itg_sample_raw_database):
    items = [item.columns for item in cdf_client.raw.rows.list(itg_sample_raw_database, "Equipment")]
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
    Ingest2Itg(items, "Equipment", "test")

#    print("--- Area")
#    items = [item.columns for item in cdf_client.raw.rows.list(itg_sample_raw_database, "Area")]
#    for item in items:
#        print(item)
#
#    print("--- put equipments to object")
#    for item in items:
#        #item['equipments'] = {'id': item['equipments']}
#        del item['equipments']
#    for item in items:
#        print(item)
#
#    Ingest2Itg(items, "Area")

def TestItg1692():
    item = [{
        "tag": '21pt1019',
         "isActive": False,
         "dimension": {
             "x": 0,
             "y": 0,
             "z": 1,
         }
     }]
    Ingest2Itg(item, "Itg1692Third", "test")

def Aize() :
    items = [{
  "id": "ff06475e-70d2-4705-bd53-cc790d6827e0",
  "jamaId": 18484,
  "jamaGlobalId": "GID-70145",
  "description": "This is a test requirement",
  "lastMerged": {
    "day": 30,
    "month": 6,
    "year": 2021,
    "hour": 9,
    "minute": 11
  },
  "modifiedDate": {
    "day": 19,
    "month": 5,
    "year": 2021,
    "hour": 9,
    "minute": 50
  },
  "name": "Test Functional requirement",
  "projectId": "56",
  "status": "Draft",
  "tags": [],
  "type": "Functional Requirement"
    }]

    Ingest2Itg(items, "Requirement", "test")

if __name__=="__main__":
   itg_sample_raw_database = "ItgSampleData13"

   client_name = "ingestion-pipeline"

   cdf_client = CogniteClient(api_key=api_key, client_name=client_name, project=cdf_tenant)

   #IngestRaw(cdf_client, itg_sample_raw_database)

   #FetchRaw(cdf_client, itg_sample_raw_database)

   #TransformIngest(cdf_client, itg_sample_raw_database)

   #TestItg1692()

   Aize()

