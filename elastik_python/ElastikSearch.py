import warnings
from elasticsearch import Elasticsearch, RequestsHttpConnection
warnings.filterwarnings('ignore')
import requests

res = requests.get('http://localhost:9200?pretty')
print(res.content)
es = Elasticsearch('http://localhost:9200')
