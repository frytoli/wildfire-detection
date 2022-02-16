#!/usr/bin/env python3

# Airflow
from airflow.contrib.operators.redis_publish_operator import RedisPublishOperator
from airflow.models import Variable
from airflow import DAG

# Other
import datetime
import db

# Initialize database object
mdb = db.mongo(
	Variable.get('DB_HOST'),
	Variable.get('DB_PORT'),
	Variable.get('DB_USER'),
	Variable.get('DB_PASS'),
	Variable.get('DB_NAME')
)

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime.datetime(2020, 1, 1),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': datetime.timedelta(seconds=5)
}

# Build the DAG
DAG = DAG(
	dag_id = 'alertwildfire-classic-allregions-scheduler',
	description = 'Push all ALERTWildfire regions to the Redis queue on a regular cadence to be scraped',
	default_args = default_args,
	concurrency = 1,
	max_active_runs = 1,
	catchup = False,
	schedule_interval = '* 0-23/4 * * *', # Every 4 hours
	dagrun_timeout=datetime.timedelta(days=1) # 24 hour timeout
)
opr_redis_publish = RedisPublishOperator(
	task_id = 'redis-publish',
	channel = Variable.get('ALERTWILDFIRE_CHANNEL'),
	redis_conn_id = 'redis-default',
	message = ','.join(mdb.get_all_regions()),
	dag = DAG
)

# ========================================================================

opr_redis_publish
