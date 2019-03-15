from datetime import datetime, timedelta, tzinfo
import elasticsearch
import csv, json
import subprocess
import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def extract_data_from_es(date_from, date_to, local_path_csv):

    # extract batch from ES
    es = elasticsearch.Elasticsearch(["34.76.45.249:9200"])
    res = es.search(index="ilya.kobelev", body={
        "query": {
            "range":
            {"@timestamp": {
                "gte": date_from,
                "lt": date_to,
                "time_zone": "UTC"
            }
            }
        }
    }, size=10000)

    # dump batch to local csv file
    with open(local_path_csv, 'w') as f:  
        header_present = False
        for doc in res['hits']['hits']:
            msg = json.loads(doc['_source']['message'])
            if not header_present:
                w = csv.DictWriter(f, msg.keys())
                w.writeheader()
                header_present = True

            w.writerow(msg)


def dag_task(ds, **kwargs):

    local_file_dir = '/home/ikobelev/lab01'

    # read dagrun parameters
    execution_date = kwargs['execution_date']
    next_execution_date = kwargs['next_execution_date']
    dag_run_id = kwargs['run_id']

    # extract batch from elasticserach
    extract_data_from_es(execution_date, next_execution_date,
                         '{0}/{1}.csv'.format(local_file_dir, dag_run_id))

    # at the end of day merge 15-mins batches into single file and put to HDFS
    if execution_date.hour == 23 and execution_date.minute == 45:
        
        merged_file_name='es_{0}.csv'.format(execution_date.strftime('%Y-%m-%d'))

        # merge 15-mins files into single one
        subprocess.check_call('cat {0}/sched*.csv > {0}/{1}'.format(local_file_dir,merged_file_name), shell=True)
        subprocess.check_call('rm {0}/sched*.csv -f'.format(local_file_dir), shell=True)

        #copy merged file to HDFS
        subprocess.check_call("hdfs dfs -copyFromLocal -f {0}/{1} /opt/project".format(local_file_dir,merged_file_name), shell=True)

        #import merged file into Click House
        subprocess.check_call('cat {0}/{1} | clickhouse-client --query="INSERT INTO default.ilya_kobelev FORMAT CSV"'.format(local_file_dir,merged_file_name), shell=True)



dag = DAG('lab1_dag',
          description='DE 4.0 Lab 01 DAG',
          schedule_interval='0,15,30,45 * * * *',
          start_date=datetime(2019, 3, 13, 21, 00, tzinfo=pytz.utc), catchup=True)
dag_operator = PythonOperator(
    task_id='load_from_es_to_hdfs_and_clickhouse', python_callable=dag_task, provide_context=True, dag=dag)
dag_operator

