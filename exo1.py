import requests
from pprint import pprint
from pymongo import MongoClient, GEOSPHERE


client = MongoClient("mongodb+srv://youssef:hohaj@cluster0.fjqjc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.test


myDb = client["veloDb"]
collection = myDb["VeloCol"]

# Cleaning dataBase before start
collection.drop()
collection.create_index([("location",GEOSPHERE)])

def lille_provider():
	url="https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
	response = requests.get(url).json()
	stations_data=response['records']
	return stations_data

def paris_provider():
	url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=2000&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes&refine.nom_arrondissement_communes=Paris"
	response = requests.get(url).json()
	stations_data=response['records']
	return stations_data

def rennes_provider():
	url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&rows=2000&facet=nom&facet=etat&facet=nombreemplacementsactuels&facet=nombreemplacementsdisponibles&facet=nombrevelosdisponibles"
	response = requests.get(url).json()
	stations_data=response['records']
	return stations_data

def lyon_provider():
	url = "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json?maxfeatures=-1&start=1"
	response = requests.get(url).json()
	stations_data=response['records']
	return stations_data

def lille_consumer(collection):
	stations = lille_provider()
	localisation=[]
	for input_station in stations:
		input_tab=input_station["fields"]["coordonnees"]
		localisation.append(input_tab)

	#Récupération des données de l'api
	list_station=[]
	i=0
	for input_station in stations:
		input_json={"ville":"Lille","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":localisation[i]}}
		list_station.append(input_json)
		i=i+1

	#Ajout des données dans la BDD
	collection.insert_many(list_station)

def paris_consumer(collection):
	stations = paris_provider()
	localisation=[]
	for input_station in stations:
		input_tab=input_station["fields"]["localisation"]
		localisation.append(input_tab)

	#Récupération des données de l'api
	list_station=[]
	i=0
	for input_station in stations:
		input_json={"ville":"Paris","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":localisation[i]}}
		list_station.append(input_json)
		i=i+1

	#Ajout des données dans la BDD
	collection.insert_many(list_station)

def rennes_consumer(collection):
	stations = rennes_provider()
	localisation=[]
	for input_station in stations:
		input_tab=input_station["fields"]["localisation"]
		localisation.append(input_tab)

	#Récupération des données de l'api
	list_station=[]
	i=0
	for input_station in stations:
		input_json={"ville":"Rennes","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":localisation[i]}}
		list_station.append(input_json)
		i=i+1

	#Ajout des données dans la BDD
	collection.insert_many(list_station)

def lyon_consumer(collection):
	stations = lyon_provider()
	localisation=[]
	for input_station in stations:
		input_tab=input_station["fields"]["localisation"]
		localisation.append(input_tab)

	#Récupération des données de l'api
	list_station=[]
	i=0
	for input_station in stations:
		input_json={"ville":"Lyon","nomstation":input_station["fields"]["nom"],"nombrevelo":input_station["fields"]["nbvelosdispo"],"nombreplaces":input_station["fields"]["nbplacesdispo"],"etat":input_station["fields"]["etat"],"location":{"type":"Point", "coordinates":localisation[i]}}
		list_station.append(input_json)
		i=i+1

	#Ajout des données dans la BDD
	collection.insert_many(list_station)

def provider():
	lille_provider()
	paris_provider()
	rennes_consumer()
	lyon_consumer()

def consumer(collection):
	lille_consumer(collection)
	paris_consumer(collection)
	rennes_consumer(collection)
	lyon_consumer(collection)

if __name__ == "__main__":
	provider()
	consumer(collection)