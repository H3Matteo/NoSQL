from pymongo import MongoClient
import json

client = MongoClient('mongodb://localhost:27017/')

db = client.mydb
collection = db.mycollection

with open("accounts.json", "r") as file:
    data = json.load(file)

result = collection.insert_many(data)

#print("Inserted data with the following IDs:", result.inserted_ids)

index_name = "country_index"
collection.create_index("address.country", name=index_name)

country = "Indonesia"
results = collection.find({"country" : country})

for result in results:
        print("rechere par pays :", result)
min_currenty = 95

results = collection.find({
    "$expr": {
        "$gt": [
            {"$toDouble": {"$substr": ["$currency", 1, -1]}},  # Convertit "$22.07" en 22.07
            min_currenty
        ]
    }
})

for result in results:
    print("resulta pour Au dessus de 95 dollar:", result)

#total argent par pays

pipeline = [
    {
        "$group": {
            "_id": "$country",  # Regroupement par pays
            "total_money": {
                "$sum": {
                    "$toDouble": {"$substr": ["$currency", 1, -1]}  # Convertit "$22.07" en 22.07 et additionne
                }
            }
        }
    }
]

results = collection.aggregate(pipeline)

for result in results:
    print(f"Pays: {result['_id']}, Total: ${result['total_money']:.2f}")

# Agrégation pour compter le nombre de comptes par pays
pipeline = [
    {
        "$group": {
            "_id": "$country",  # Regroupement par pays
            "total_accounts": {"$sum": 1}  # Compte le nombre de comptes
        }
    }
]

results = collection.aggregate(pipeline)

for result in results:
    print(f"Pays: {result['_id']}, Nombre de comptes: {result['total_accounts']}")

#total compte par numberrange et total
pipeline = [
    {"$match": {"numberrange": {"$gt": 8}}},  # Filtrer les documents avec numberrange > 8
    {"$group": {"_id": "$numberrange", "count": {"$sum": 1}}}  # Grouper par numberrange et compter
]

results = collection.aggregate(pipeline)

for result in results:
    print(f"Valeur numberrange: {result['_id']}, Occurrences: {result['count']}")