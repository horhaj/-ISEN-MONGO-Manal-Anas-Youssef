import logging
import json
from logging import debug, raiseExceptions

from requests.models import Response
from project.adapters.api.client.client_requests_adapters import ClientRequest
from project.entities.requests import Request
import requests
from pprint import pprint
from pymongo import MongoClient, GEOSPHERE


client = MongoClient("mongodb+srv://youssef:hohaj@cluster0.fjqjc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.test


myDb = client["veloDb"]
collection = myDb["VeloCol"]
collection.find_one()
#mongodb+srv://myself:<password>@cluster0.fjqjc.mongodb.net/test

collection.drop()
collection.create_index([("location",GEOSPHERE)])

def lille_provider():
	url="https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
	response = requests.get(url).json()
	infos_stations=response['records']
	return infos_stations

def lille_consumer():
	stations = lille_provider()
	lilleTab1=[]
	for input_station in stations:
		input_tab=input_station["fields"]["localisation"]
		lilleTab1.append(input_tab)

	#Récupération des données de l'api
	lilleTab=[]
	i=0
	for input_station in stations:
		input_json={"ville":"Lille","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":lilleTab1[i]}}
		lilleTab.append(input_json)
		i=i+1

	#Ajout des données dans la BDD
	collection.insert_many(lilleTab)
	
def lille_updater():
	stations = lille_provider()
	lilleTab1=[]
	for input_station in stations:
		input_tab=input_station["fields"]["localisation"]
		lilleTab1.append(input_tab)

	stations = lille_provider()
	i=0
	for input_station in stations:
		stat={"ville":"Lille","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],
		"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":lilleTab1[i]}}
		collection.update_one({"nomstation": stat["nomstation"] }, {"$set": stat}, upsert=True)	
		i += 1


def get_DS():
    url ="https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=1000&start=0&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET",url)
    response_json = json.loads(response.text.encode("utf8"))
    return response_json.get("records", [])


def update(time):
    client = pymongo.MongoClient("mongodb+srv://saad:saad@vlille.pvgiw.mongodb.net/vlille-shard-00-02.pvgiw.mongodb.net:27017?retryWrites=true&w=majority")
    collection = client["vlille"]
    db = collection["test"]
    while True:
        data = get_DS()
        x = db.insert_many(data)
        time.sleep(time*60)
# r = requests.get('https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=box-a-velos-a-lille-lomme-et-hellemmes&q=&facet=rue&facet=villes')

# print(r.text)
# print(r.json())
stations = lille_provider()
lilleTab1=[]
for input_station in stations:
	input_tab=input_station["fields"]["localisation"]
	lilleTab1.append(input_tab)
lilleTab1
#Récupération des données de l'api
lilleTab=[]
i=0
for input_station in stations:
	input_json={"ville":"Lille","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":lilleTab1[i]}}
	lilleTab.append(input_json)
	i=i+1
lilleTab
#Ajout des données dans la BDD
collection.insert_many(lilleTab)

stat =lilleTab[1]
stat["nombrevelo"] = 5
collection.update_one({ "nomstation": stat["nomstation"] }, {"$set": stat}, upsert=True)
