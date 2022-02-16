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
import datetime

def invoke_model(**kwargs):
	from model.yolo3.yolo import YOLO, detect_video
	from PIL import ImageFile
	from PIL import Image
	import numpy as np
	import base64
	import drive
	import time
	import db
	import io
	import os

	ImageFile.LOAD_TRUNCATED_IMAGES = True

	# Retrieve published image via Redis Pub-Sub Sensor
	message = kwargs['ti'].xcom_pull('redis-sensor', key='message')
	print(f'Successfully received message: {message}')

	# Split message on delimiter and get id/region/image
	data = message['data'].split(b'   ')
	id, region, image = data[0].decode('utf-8'), data[1].decode('utf-8'), data[2]
	# Decode image to bytes
	try:
		image = base64.b64decode(image)
	except:
		raise AirflowFailException('Received image could not be base64 decoded')

	# Initialize database object
	mdb = db.mongo(
		Variable.get('DB_HOST'),
		Variable.get('DB_PORT'),
		Variable.get('DB_USER'),
		Variable.get('DB_PASS'),
		Variable.get('DB_NAME')
	)
	# Insert record into database
	mdb.insert_detection(id)

	# Initialize a fire detection YOLO object
	fire_yolo = YOLO(
		**{
			'model_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'fire.h5'), # If built with Docker, the model's name is always "fire.h5" -- See Dockerfile
			'anchors_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'yolo3', 'yolo_anchors.txt'),
			'classes_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'fire_classes.txt'),
			'score': 0.25,
			'gpu': 1,
			'model_image_size': (416, 416)
		}
	)

	# Initialize a smoke detection YOLO object
	smoke_yolo = YOLO(
		**{
			'model_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'smoke.h5'), # If built with Docker, the model's name is always "smoke.h5" -- See Dockerfile
			'anchors_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'yolo3', 'yolo_anchors.txt'),
			'classes_path': os.path.join(os.getenv('AIRFLOW_HOME'), 'dags', 'model', 'smoke_classes.txt'),
			'score': 0.25,
			'gpu': 1,
			'model_image_size': (416, 416)
		}
	)

	# Convert bytes to PIL Image
	try:
		image = Image.open(io.BytesIO(image))
		if image.mode != 'RGB':
			image = image.convert('RGB')
	except:
		raise AirflowFailException('Could not convert byte array to PIL Image')

	# Detect fire in image
	start = time.time()
	fire_prediction, new_image = fire_yolo.detect_image(image)
	fire_elapsed = time.time()-start
	print(f'[-] Fire detection completed in {fire_elapsed} seconds')

	# Detect smoke in new image
	start = time.time()
	smoke_prediction, new_image = smoke_yolo.detect_image(new_image)
	smoke_elapsed = time.time()-start
	print(f'[-] Smoke detection completed in {smoke_elapsed} seconds')

	# Set timestamp
	timestamp = datetime.datetime.utcnow()

	#print(prediction, y_size, x_size)
	if len(fire_prediction) > 0 or len(smoke_prediction) > 0:
		print('Fire and/or smoke detected in image')
		# Load prediction image into buffer
		buffered = io.BytesIO()
		new_image.save(buffered, format='JPEG')
		# Save image to Google Drive
		gd = drive.gdrive()
		# Upload prediction image from buffer
		gd.upload_buffer(buffered, f'{region}_{id}_{timestamp}.jpg', mimetype='image/jpg')
		# Update database record
		new_image_str = base64.b64encode(buffered.getvalue())
		mdb.update_detection(id, timestamp, fire_elapsed, smoke_elapsed, True, image=new_image_str)
		return True, region
	else:
		print('No fire detected in image')
		# Update database record
		mdb.update_detection(id, timestamp, fire_elapsed, smoke_elapsed, False)
		return False, region

def branch(**kwargs):
	continue_to_alert, region = kwargs['ti'].xcom_pull('invoke-model')
	if continue_to_alert:
		return 'alert'
	else:
		return 'no-alert'

def alert(**kwargs):
	'''
	Push region to the ALERTWildfire channel to be scraped again and (not yet implemented) alert the fire department
	'''
	import redis

	# Retrieve region
	continue_to_alert, region = kwargs['ti'].xcom_pull('invoke-model')

	# Connect to redis and push to queue
	r = redis.Redis(host=Variable.get('REDIS_BACKEND_HOST'), port=Variable.get('REDIS_BACKEND_PORT'))
	r.publish(Variable.get('ALERTWILDFIRE_CHANNEL'), region)

	# Alert the fire department
	print(f'Now alerting the fire department in region {region}')

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
	channels = Variable.get('DETECTION_CHANNEL'),
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
