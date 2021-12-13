#!/usr/bin/env python3

# Airflow
from airflow.contrib.operators.redis_publish_operator import RedisPublishOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow import DAG

# Other
import datetime
import asyncio
import random
import time
import os
import db

# Initialize database object
mdb = db.mongo(
	Variable.get('DB_HOST'),
	Variable.get('DB_PORT'),
	Variable.get('DB_USER'),
	Variable.get('DB_PASS'),
	Variable.get('DB_NAME')
)

def pull_regions(**kwargs):
	'''
	Pull messages (regions) from redis over a 10-minute period, then send the regions to next operator
	'''
	import redis

	# Connect to redis and subscribe to queue
	r = redis.Redis(host=Variable.get('REDIS_BACKEND_HOST'), port=Variable.get('REDIS_BACKEND_PORT'))
	queue = r.pubsub()
	queue.subscribe(Variable.get('ALERTWILDFIRE_CHANNEL'))

	regions = set()
	start_time = time.time()
	while time.time()-start_time < 600: # 10 minutes
		# Fetch a message
		message = queue.get_message()
		# If a new message was retrieved...
		if message and message['type'] == 'message':
			# Add regions
			regions = regions.union(set(message['data'].decode('utf-8').strip().split(',')))
		time.sleep(1)
	# Once 10 minutes has passed, return the regions
	return regions

def branch(**kwargs):
	'''
	Decide if regions have been returned and if scraping needs to occur
	'''
	# Retrieve regions from previous operator
	regions = kwargs['ti'].xcom_pull('pull-regions')

	# Scrape if regions had been returned, otherwise trigger a new DAG
	if len(regions) > 0:
		return 'prep-regions'
	else:
		return 'trigger-newdag2'

def prep_regions(**kwargs):
	'''
	Retrieve documents filtered by provided region and push to xcom for scrapers
	'''
	# Retrieve regions from previous operator
	regions = kwargs['ti'].xcom_pull('pull-regions')

	# Iterate over regions, retrieve docs from mongo, and push to xcom
	for region in regions:
		docs = mdb.get_docs('alertwildfire-cameras', filter={'region':region})
		# Get current epoch time
		epoch = int(time.time())
		# Push epoch timestamp and docs to xcom
		kwargs['ti'].xcom_push(key=f'{region}-docs', value=[epoch, docs])

def _get_proxies():
	'''
		Fetch a free list of http proxies from Proxyscrape.com.

		Returns:
		A list of valid proxy:port pair strings.
	'''
	from requests_html import HTMLSession

	# Initialize HTML session
	session = HTMLSession()
	# Request data from proxyscrape API
	try:
		api_resp = session.get(
			'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all',
			timeout=30
		).text
	except Exception as e:
		# If there's an error, sleep and try again
		time.sleep(random.randint(3,6))
		try:
			api_resp = session.get(
				'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all',
				timeout=30
			).text
		except Exception as e:
			api_resp = None
	# Close session
	session.close()
	# If a good response was retrieved, process the list
	if api_resp:
		# Split response blob into proxy ip:port pairs
		pairs = api_resp.split('\r\n')
		# Remove empty values or mis-structured values
		good_pairs = [x for x in pairs if (x != '' and x.count('.') == 3)]
	# Otherwise, return an empty list
	else:
		good_pairs = []
	# Return good proxy pairs
	return good_pairs

def _find_closest_request_time(desired, base, delta):
	'''
	Find the closest time to the desired time given a starting base time and time delta in seconds.

	Args:
	`desired`: [int] The desired epoch time (in seconds) which evaluate the closest time given the base time and time delta
	`base`: [int] The base epoch time (in seconds)
	`delta`: [int] The time delta (in seconds)
	Returns:
	The closest time to the desired time given the starting base time and time delta.
	'''
	# Base time is always < desired time
	return int(base+(((desired-base)//delta)*delta))

def _make_afunc(asession, id, url, proxy, headers={}, render=False):
	'''
		Dynamically build an asynchronous function at runtime for use by requests-html's AsyncHTMLSession's run() method

		Args:
			asession: (AsyncHTMLSession) the AsyncHTMLSession object
			id: (str) the document's id
			url: (str) the docuemnt's url to the camera
			proxy: (str) a proxy:port pair
			headers: (dict) (optional) dictionary of desired request headers
			render: (bool) (optional) indicator to render (or not) the javascript of the request's returned html

		Returns:
			asynchronous function
				Returns:
					(requests-html response object)
					(str) the document's id
					(str) the docuemnt's url to the camera
					(str) the provided proxy:port pair
	'''
	async def _afunction():
		r = None
		try:
			# Add headers if applicable
			if len(headers) > 0:
				r = await asession.get(
					url,
					proxies={
						'http':f'http://{proxy}',
						'https':f'https://{proxy}'
					},
					headers=headers,
					timeout=30
				)
			else:
				r = await asession.get(
					url,
					proxies={
						'http':f'http://{proxy}',
						'https':f'https://{proxy}'
					},
					timeout=30
				)
			# Render JS (This can raise a pyppeteer TimeoutError)
			if render and r:
				await r.html.arender(timeout=30, sleep=random.randint(2,5))
		#except (AttributeError, ConnectionError, ProxyError, TooManyRedirects, ReadTimeout, pyppeteer.errors.TimeoutError, pyppeteer.errors.BrowserError, pyppeteer.errors.ConnectionError, ParserError, CancelledError, InvalidStateError) as e:
		except Exception as e:
			print(f'  [!] Error: {e}')
			r = None
		return r, id, url
	return _afunction

def scrape_classic(**kwargs):
	'''
		Asynchronous scrape and save images from a group of camera urls

		Args:
			saveto_dir: (str) path to the directory where the images are to be saved
			docs: (list(dic())) a list of json documents with keys "id" and "url"
			timeout: (int) (optional) number of seconds until timeout; this defaults to 50 minutes, 10 minutes less than RabbitMQ's timeout

		Returns:
			dictionary object of {"success": int() successful job count, "failure":int() failed job count}
	'''
	from requests_html import AsyncHTMLSession
	import base64

	# Pull chunk from xcom
	scraper_region = kwargs['ti'].task_id.split('.')[-1].split('-')[0]
	message = kwargs['ti'].xcom_pull(key=f'{scraper_region}-docs', task_ids='prep-regions')
	if message:
		epoch = message[0]
		cameras = message[1]
		# Instantiate success and failure count objects
		success = 0
		failure = len(cameras)
		# Set timeout to 30 minutes (seconds)
		timeout = 1800
		# Setup timer
		start = time.time()
		elapsed = time.time()-start
		# Fetch proxies
		proxies = _get_proxies()
		# If request to proxyscrape was bad, sleep and try again
		if len(proxies) < 1:
			# Sleep randomly
			time.sleep(random.randint(13,30))
			# Try again
			proxies = _get_proxies()
		# Make sure proxies were acquired successfully, otherwise report failure
		if len(proxies) == 0:
			raise AirflowException('List of proxies is empty')
		# Initialize records
		active = {cam['id']: {'url': f'''{cam['url']}{_find_closest_request_time(epoch, cam['request_time'], cam['request_time_delta'])}'''} for cam in cameras}
		# Initialize temp Async HTML session
		asession = None
		# Loop until all good responses are found
		task_num = 1
		while len(active) > 0 and elapsed < timeout:
			# Sleep randomly
			time.sleep(random.randint(13,30))
			# Initialize Async HTML session (sometimes these break)
			if not asession:
				try:
					# Set existing event loop
					loop = asyncio.get_event_loop()
				except RuntimeError:
					# If no event loop exists, create/set a new one
					loop = asyncio.new_event_loop()
					asyncio.set_event_loop(loop)
				asession = AsyncHTMLSession(loop=loop)
			# Craft dynamic arguments for async session
			aargs = []
			for id in active:
				camera = active[id]
				# Make async function for image page request
				aargs.append(_make_afunc(asession, id, camera['url'], random.choice(proxies), headers={'Referer': 'http://www.alertwildfire.org/'}, render=True))
			# Make async requests and pair with id
			results = asession.run(*aargs)
			# Iterate over results and push image to queue is scraping was successful
			for result in results:
				# Parse result for ease of use
				r = result[0]
				id = result[1]
				url = result[2]
				if r and r.status_code in [200, 301, 302, 303, 307]:
					print(f'[-] Good response from {url}')
					# Publish base64 encoded image string as a message to the Redis fire-detection queue with a RedisPublishOperator
					opr_redis_publish = RedisPublishOperator(
						task_id = f'redis-publish-{scraper_region}.{task_num}',
						channel = 'redis-detection-channel',
						redis_conn_id = 'redis-default',
						message = f'''{id}   {scraper_region}   {base64.b64encode(r.content).decode('utf-8')}''',
						dag = DAG
					).execute(context=kwargs)
					print(f'Pushed image {id} to detection queue')
					task_num += 1
					# Update record, aka remove it from the list of active tasks
					del active[id]
					# Increment success and decrement failure
					success += 1
					failure -= 1
				# Close request object
				if r:
					r.close()
				# Update elapsed time
				elapsed = time.time()-start
		print(f'[-] Elapsed time: {elapsed}')
		# Close browser
		asyncio.run(asession.close())
		# Print successes/failures
		print(f'Successes: {success}, Failures: {failure}')

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
	dag_id = 'alertwildfire-classic-scraper',
	description = 'ALERTWildfire classic camera image scraper invoked upon observation of new tweets @ALERTWildfire',
	default_args = default_args,
	concurrency = 4,
	max_active_runs = 2,
	catchup = False,
	schedule_interval = '@once', # One time - DAG is triggered by itself upon retrieving a new message from the queue
	dagrun_timeout=datetime.timedelta(days=1) # 24 hour timeout
)
opr_pull_regions = PythonOperator(
	task_id = 'pull-regions',
	python_callable = pull_regions,
	provide_context = True,
	dag = DAG
)
opr_branch = BranchPythonOperator(
	task_id = 'branch',
	python_callable = branch,
	provide_context = True,
	dag = DAG
)
opr_prep_regions = PythonOperator(
	task_id = 'prep-regions',
	python_callable = prep_regions,
	provide_context = True,
	dag = DAG
)
opr_trigger_newdag = TriggerDagRunOperator(
	task_id = 'trigger-newdag',
	trigger_dag_id = 'alertwildfire-classic-scraper',
	wait_for_completion = False,
	dag = DAG
)
opr_trigger_newdag2 = TriggerDagRunOperator(
	task_id = 'trigger-newdag2',
	trigger_dag_id = 'alertwildfire-classic-scraper',
	wait_for_completion = False,
	dag = DAG
)
# Dynamically create region scrapers
regions = mdb.get_all_regions()
group = []
# Create task group
with TaskGroup(
	group_id = 'scraper-group',
	dag = DAG) as scraper_group:
	for region in regions:
		group.append(
			PythonOperator(
				task_id = f'{region}-scrape-classic',
				python_callable = scrape_classic,
				provide_context = True,
				dag = DAG
			)
		)

# ========================================================================

opr_pull_regions >> opr_branch >> opr_prep_regions >> scraper_group >> opr_trigger_newdag
opr_pull_regions >> opr_branch >> opr_trigger_newdag2
