from datetime import datetime
import elasticsearch
import csv
import unicodedata
from urllib.parse import urlparse


def pull_from_elasticsearch():
    es = elasticsearch.Elasticsearch(["34.76.45.249:9200"])
    res = es.search(index="ilya.kobelev", body={"query": {"match_all": {}}})
    sample = res['hits']['hits']


pull_from_elasticsearch()
