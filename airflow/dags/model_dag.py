#!/usr/bin/env python3

# Airflow
from airflow.providers.redis.sensors.redis_pub_sub import RedisPubSubSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow import DAG

# Other
from model.yolo3.yolo import YOLO, detect_video
from PIL import Image
import numpy as np
import datetime
import logging
import base64
import random
import json
import time
import db
import io
import os

def invoke_model(**kwargs):
	# Retrieve published image via Redis Pub-Sub Sensor
	message = kwargs['ti'].xcom_pull('redis-sensor', key='message')
	print(f'Successfully received message: {message}')

	# Get base64 encoded image string
	image = message['data']
	# Decode image to bytes
	try:
		image = base64.b64decode(image)
	except:
		raise AirflowFailException('Received image could not be base64 decoded')

	# Initialize database object
	adb = db.arangodb(
		Variable.get('DB_HOST'),
		Variable.get('DB_PORT'),
		Variable.get('DB_USER'),
		Variable.get('DB_PASS'),
		Variable.get('DB_NAME')
	)
	# Insert record into database
	id = adb.insert_image()

	# Initialize a golbal YOLO object
	yolo = YOLO(
		**{
			'model_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'yolo.h5'), # If built with Docker, the model's name is always "yolo.h5" -- See Dockerfile
			'anchors_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'yolo3', 'yolo_anchors.txt'),
			'classes_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'data_classes.txt'),
			'score': 0.25,
			'gpu': 1,
			'model_image_size': (416, 416)
		}
	)

	start = time.time()
	# Convert bytes to PIL Image
	try:
		image = Image.open(io.BytesIO(image))
		if image.mode != 'RGB':
			image = image.convert('RGB')
	except:
		raise AirflowFailException('Could not convert byte array to PIL Image')

	# Detect fire in image with globall yolo object
	prediction, new_image = yolo.detect_image(image)

	# Get x and y sizes
	y_size, x_size, _ = np.array(new_image).shape

	# Get elpased and completion time
	elapsed = time.time()-start
	timestamp = datetime.datetime.utcnow()
	print(f'Detection completed in {elapsed} seconds')

	#print(prediction, y_size, x_size)
	if len(prediction) > 0:
		print('Fire detected in image')
		# Update database record
		buffered = io.BytesIO()
		new_image.save(buffered, format='JPEG')
		new_image_str = base64.b64encode(buffered.getvalue())
		adb.update_image(id, timestamp, elapsed, True, image=new_image_str)
		return True
	else:
		print('No fire detected in image')
		# Update database record
		adb.update_image(id, timestamp, elapsed, False)
		return False

def branch(**kwargs):
	continue_to_alert = kwargs['ti'].xcom_pull('invoke-model')
	if continue_to_alert:
		return 'alert'
	else:
		return 'no-alert'

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
	'retry_delay': datetime.timedelta(seconds=5)
}

# Build the DAG
DAG = DAG(
	dag_id = 'fire-detection-and-alert',
	description = 'Fire detection in provided images with the trained model and consequential alerting of appropriate parties upon True observations',
	default_args = default_args,
	concurrency = 15,
	max_active_runs = 30,
	catchup = False,
	schedule_interval = '@once', # One time - DAG is triggered by itself upon retrieving a new message from the queue
	dagrun_timeout=datetime.timedelta(days=1) # 24 hour timeout
)
snsr_redis_pubsub = RedisPubSubSensor(
	task_id = 'redis-sensor',
	channels = 'redis-detection-channel',
	redis_conn_id = 'redis-default',
	dag = DAG
)
opr_invoke_model = PythonOperator(
	task_id = 'invoke-model',
	provide_context = True,
	python_callable = invoke_model,
	dag = DAG
)
opr_trigger_newdag = TriggerDagRunOperator(
	task_id = 'trigger-newdag',
	trigger_dag_id = 'fire-detection-and-alert',
	wait_for_completion = False,
	dag = DAG
)
branch = BranchPythonOperator(
	task_id = 'branch',
	provide_context = True,
	python_callable = branch,
	dag = DAG
)
opr_noalert_dummy = DummyOperator(
	task_id = 'no-alert',
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

snsr_redis_pubsub >> opr_trigger_newdag
snsr_redis_pubsub >> opr_invoke_model >> branch >> [opr_noalert_dummy, opr_alert]
