from datetime import datetime, timedelta, tzinfo
import elasticsearch
import json
import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def pull_from_elasticsearch(ds, **kwargs):
    es = elasticsearch.Elasticsearch(["34.76.45.249:9200"])
    res = es.search(index="ilya.kobelev", body={
        "query": {
            "range":
                {"@timestamp": {
                    "gte": kwargs['execution_date']-timedelta(minutes=15),
                    "lt": kwargs['execution_date']
                }
            }
        }
    })
    with open('query_result.json', 'w') as f:
        json.dump(res['hits']['hits'], f, ensure_ascii=False)

dag = DAG('lab1_dag', description='Simple tutorial DAG',
          schedule_interval='0,15,30,45 * * * *',          
          start_date=datetime(2019, 3, 13, 15, 30, tzinfo=pytz.utc), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

pull_from_elasticsearch_operator = PythonOperator(task_id='pull_from_elasticsearch_task', python_callable=pull_from_elasticsearch,provide_context=True, dag=dag)

dummy_operator >> pull_from_elasticsearch_operator


