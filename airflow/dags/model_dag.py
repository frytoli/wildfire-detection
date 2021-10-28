#!/usr/bin/env python3

# Airflow
from airflow.providers.redis.sensors.redis_pub_sub import RedisPubSubSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow import DAG

# Other
import datetime
import asyncio
import logging
import random
import json
import time
#import db
import re

def invoke_model(**kwargs):
    # Retrieve published image via Redis Pub-Sub Sensor
    message = json.loads(kwargs['ti'].xcom_pull('redis_sensor', key='message'))
    print(f'Successfully recieved message: {message}')

def branch(**kwargs):
    return 'alert'

def alert(**kwargs):
    print('Now alerting the fire department')

# ========================================================================

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime.datetime(2020, 1, 1),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': datetime.timedelta(seconds=5),
	'concurrency': 2
}

# Build the DAG
DAG = DAG(
	dag_id = 'fire-detection-and-alert',
	description = 'Fire detection in provided images with the trained model and consequential alerting of appropriate parties upon True observations',
	default_args = default_args,
	catchup = False,
	schedule_interval = '@once',
	dagrun_timeout=datetime.timedelta(days=1) # 24 hour timeout
)
snsr_redis_pubsub = RedisPubSubSensor(
    task_id = 'redis-sensor',
    channels = Variable.get('redis-detection-channel'),
    redis_conn_id = 'redis_default',
    dag = DAG
)
opr_invoke_model = PythonOperator(
    task_id = 'invoke-model',
    provide_context = True,
    python_callable = invoke_model,
    dag = DAG
)
branch = BranchPythonOperator(
    task_id = 'branch',
    provide_context = True,
    python_callable = branch,
    dag = DAG
)
# This can be converted to a SubDAG
opr_alert = PythonOperator(
    task_id = 'alert',
    provide_context = True,
    python_callable = alert,
    dag = DAG
)

# ========================================================================

snsr_redis_pubsub >> opr_invoke_model >> branch >> opr_alert
