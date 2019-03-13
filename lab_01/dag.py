from datetime import datetime, timedelta, tzinfo
import elasticsearch
import json
import pytz, sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def pull_from_elasticsearch(ds, **kwargs):

    # extract data from elasticserach
    es = elasticsearch.Elasticsearch(["34.76.45.249:9200"])
    res = es.search(index="ilya.kobelev", body={
        "query": {
            "range":
                {"@timestamp": {
                    "gte": kwargs['execution_date'],
                    "lt": kwargs['next_execution_date'],
                    "time_zone": "UTC"
                }
            }
        }
    }, size= 10000)

    # dump data to local file
    local_file_path='/home/ikobelev/lab01/{0}.json'.format(kwargs['run_id'])
    with open(local_file_path, 'w') as f:
        json.dump(res['hits']['hits'], f, ensure_ascii=False)


dag = DAG('lab1_dag_v2',
          description='DE 4.0 Lab 01 DAG',
          schedule_interval='0,15,30,45 * * * *',
          start_date=datetime(2019, 3, 13, 21, 00, tzinfo=pytz.utc), catchup=True)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

pull_from_elasticsearch_operator = PythonOperator(
    task_id='pull_from_elasticsearch_task', python_callable=pull_from_elasticsearch, provide_context=True, dag=dag)

dummy_operator >> pull_from_elasticsearch_operator
