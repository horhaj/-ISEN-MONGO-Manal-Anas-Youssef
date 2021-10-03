# Import nécessaire
from pymongo import  MongoClient, GEOSPHERE
from pymongo.message import query
import requests

client = MongoClient("mongodb+srv://youssef:hohaj@cluster0.fjqjc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.test

myDb = client["veloDb"]
collection = myDb["VeloCol"]

# Cleaning dataBase before start
collection.drop()
collection.create_index([("location",GEOSPHERE)])

cursor = collection.find({"etat":"EN SERVICE"})

for doc in cursor:
	collection.insert_one(doc)

latitude = input("Entrer votre latitude:")
longitude = input("Entrer votre longitude:")

query = {"location":{"$near": { "$geometry": {"type":"Point", "coordinates":[float(latitude),float(longitude)]}}}}

for data in collection.find(query).limit(3):
	print(data)