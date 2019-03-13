from datetime import datetime, timedelta, tzinfo
import elasticsearch
import json
import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator    
    
es = elasticsearch.Elasticsearch(["34.76.45.249:9200"])
res = es.search(index="ilya.kobelev", body={
    "query": {
        "range":
            {"@timestamp": {
                "gte": datetime.now() - timedelta(days=1),
                "lt": datetime.now(),
                "time_zone": "UTC"
            }
        }
    }
})
with open('query_result.json', 'w') as f:
    json.dump(res['hits']['hits'], f, ensure_ascii=False)