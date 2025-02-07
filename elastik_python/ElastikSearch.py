import warnings
import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
warnings.filterwarnings('ignore')
import requests
res = requests.get('http://localhost:9200?pretty')
print(res.content)
es = Elasticsearch('http://localhost:9200')

#create
es.indices.create(index="first_index",ignore=400)

#verify
#print (es.indices.exists(index="first_index"))

#delete
#print (es.indices.delete(index="first_index", ignore=[400,404]))

#documents to insert in the elasticsearch index "cities"
doc1 = {"city":"New Delhi", "country":"India"}
doc2 = {"city":"London", "country":"England"}
doc3 = {"city":"Los Angeles", "country":"USA"}

#Inserting doc1 in id=1
es.index(index="cities", doc_type="places", id=1, body=doc1)

#Inserting doc2 in id=2
es.index(index="cities", doc_type="places", id=2, body=doc2)

#Inserting doc3 in id=3
es.index(index="cities", doc_type="places", id=3, body=doc3)

res = es.get(index="cities", doc_type="places", id=2)
#print (res)

#print (es.indices.get_mapping(index='cities'))

#endpoint _search et query
res = es.search(index="cities", body={"query":{"match_all":{}}})
#print (res)

#_source

res =  es.search(index="movies", body={
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
#print (res)

res = es.search(index="movies", body=
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

#print (res)

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

#print(should_must)

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

#print (filter)

prefix = es.search(index="cities", body={"query": {"prefix" : { "city" : "l" }}})
#print (prefix)

regex = es.search(index="cities", body={"query": {"regexp" : { "city" : ".*" }}})
#print (regex)

#afficher les cities qui commencent par L
rexex_l = es.search(index="cities", body={"query": {"regexp" : { "city" : "l.*" }}})
#print (rexex_l)

#afficher les cities qui commencent par L et terminent par n 
regex_l_n = es.search(index="cities", body={"query": {"regexp" : { "city" : "l.*n" }}})
#print (regex_l_n)

#agregation simple -> movies/years
res = es.search(index="movies",body={"aggs" : {
    "nb_par_annee" : {
        "terms" : {"field" : "fields.year"}
}}})
#print (res['aggregations'])

#agregation et stats simple -> moyennes des raitings 
res = es.search(index="movies",body={"aggs" : {
    "note_moyenne" : {
        "avg" : {"field" : "fields.rating"}
}}})
#print (res['aggregations'])

#agregation et stats simple -> stats basiques raitings/years
res = es.search(index="movies",body={"aggs" : {
    "group_year" : {
        "terms" : { "field" : "fields.year" },
        "aggs" : {
            "note_moyenne" : {"avg" : {"field" : "fields.rating"}},
            "note_min" : {"min" : {"field" : "fields.rating"}},
            "note_max" : {"max" : {"field" : "fields.rating"}}
        }
}}})
#print (res["aggregations"])

doc1 = {"city":"Bangalore", "country":"India","datetime": datetime.datetime(2018,1,1,10,20,0)} #datetime format: yyyy,MM,dd,hh,mm,ss
doc2 = {"city":"London", "country":"England","datetime": datetime.datetime(2018,1,2,22,30,0)}
doc3 = {"city":"Los Angeles", "country":"USA","datetime": datetime.datetime(2018,4,19,18,20,0)}
es.index(index="travel", id=1, body=doc1)
es.index(index="travel", id=2, body=doc2)
es.index(index="travel", id=3, body=doc3)

#specify mapping and create index 
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

res = es.indices.get_mapping(index='travel')
#print (res)

res = es.search(index="travel", body={"from": 0, "size": 0, "query": {"match_all": {}}, "aggs": {
                  "country": {
                      "date_histogram": {"field": "datetime", "calendar_interval": "year"}}}})
#print (res)

