import warnings
from elasticsearch import Elasticsearch
warnings.filterwarnings('ignore')

import requests
res = requests.get('http://localhost:9200?pretty')
print(res.content)
es = Elasticsearch('http://localhost:9200')


print("Create :", es.indices.create(index="first_index",ignore=400))

print("Verify :", es.indices.exists(index="first_index"))

print("Delete :", es.indices.delete(index="first_index", ignore=[400,404]))

doc1 = {"city":"New Delhi", "country":"India"}
doc2 = {"city":"London", "country":"England"}
doc3 = {"city":"Los Angeles", "country":"USA"}

es.index(index="cities", id=1, body=doc1)

es.index(index="cities", id=2, body=doc2)

es.index(index="cities", id=3, body=doc3)

exists = es.indices.exists(index="cities")
print(f"L'index cities existe: {exists}")

retrieve = es.get(index="cities", id=2)
print("Data avec id 2 :", retrieve)

doc = {"city":"London", "country":"England"}
print("Doc exo :", doc)

print("Mapping :", es.indices.get_mapping(index='cities'))

endpoint = es.search(index="cities", body={"query":{"match_all":{}}})
print("Endpoint search : ",endpoint)

source = es.search(index="movies", body={
  "_source": {
    "includes": [
      "*.title",
      "*.directors"
    ],
    "excludes": [
      "*.actors*",
      "*.genres"
    ]
  },
  "query": {
    "match": {
      "fields.directors": "George"
    }
  }
})

print("Search avec _source : ", source)

bool = es.search(index="movies", body=
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "fields.directors": "George"
          }
        },
        {
          "match": {
            "fields.title": "Star Wars"
          }
        }
      ]
    }
  }
})

print("Logique Booléenne : ", bool)

should_must = es.search(index="movies", body=
{
  "query": {
    "bool": {
      "must": [
                  { "match": { "fields.title": "Star Wars"}}
                  
      ],
      "must_not": { "match": { "fields.directors": "George Miller" }},
      "should": [
                  { "match": { "fields.title": "Star" }},
                  { "match": { "fields.directors": "George Lucas"}}
      ]
    }
  }
})

print("Logique Booléenne avec should_must : ", should_must)


filter = es.search(index="receipe", body={
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "ingredients.name": "parmesan"
          }
        }
      ], 
      "must_not": [
        {
          "match": {
            "ingredients.name": "tuna"
          }
        }
      ], 
      "filter": [
        {
          "range":{
            "preparation_time_minutes": {
              "lte":15
            }
          }
        }
        ]
    }
  }
})

print("Logique Booléenne avec filter : ", filter)

prefix = es.search(index="cities", body={"query": {"prefix" : { "city" : "l" }}})
print("Prefix : ", prefix)

regex_all = es.search(index="cities", body={"query": {"regexp" : { "city" : ".*" }}})
print("Regex : ", regex_all)

regex_cities = es.search(index="cities", body={"query": {"regexp" : { "city" : "l.*" }}})
print("Regex cities qui commencent par L : ", regex_cities)

regex_cities_End = es.search(index="cities", body={"query": {"regexp" : { "city" : "l.*n" }}})
print("Regex cities qui commencent par L et terminent par n : ", regex_cities_End)

aggregation = es.search(index="movies",body={"aggs" : {
    "nb_par_annee" : {
        "terms" : {"field" : "fields.year"}
}}})
print("Aggregation simple : ", aggregation['aggregations'])

aggregation_average = es.search(index="movies",body={"aggs" : {
    "note_moyenne" : {
        "avg" : {"field" : "fields.rating"}
}}})
print("Aggregation et stats simple : ", aggregation_average['aggregations'])

aggregation_years = es.search(index="movies",body={"aggs" : {
    "group_year" : {
        "terms" : { "field" : "fields.year" },
        "aggs" : {
            "note_moyenne" : {"avg" : {"field" : "fields.rating"}},
            "note_min" : {"min" : {"field" : "fields.rating"}},
            "note_max" : {"max" : {"field" : "fields.rating"}}
        }
}}})
print("Aggregation et stats simple : ", aggregation_years['aggregations'])

import datetime
docTime1 = {"city":"Bangalore", "country":"India","datetime": datetime.datetime(2018,1,1,10,20,0)} #datetime format: yyyy,MM,dd,hh,mm,ss
docTime2 = {"city":"London", "country":"England","datetime": datetime.datetime(2018,1,2,22,30,0)}
docTime3 = {"city":"Los Angeles", "country":"USA","datetime": datetime.datetime(2018,4,19,18,20,0)}
es.index(index="travel", id=1, body=docTime1)
es.index(index="travel", id=2, body=docTime2)
es.index(index="travel", id=3, body=docTime3)
if es.indices.exists(index="travel"):
    es.indices.delete(index="travel", ignore=[400,404])

settings = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1
    },
    "mappings": {
            "properties": {
                "city": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "country": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "datetime": {
                        "type": "date",
                    }
        }
     }
}
es.indices.create(index="travel", ignore=400, body=settings)

es.indices.get_mapping(index='travel')

es.search(index="travel", body={"from": 0, "size": 0, "query": {"match_all": {}}, "aggs": {
                  "country": {
                      "date_histogram": {"field": "datetime", "calendar_interval": "year"}}}})
