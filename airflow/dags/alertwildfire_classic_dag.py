#!/usr/bin/env python3

# Airflow
from airflow.contrib.operators.redis_publish_operator import RedisPublishOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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

# Set size of chunks
n = int(os.getenv('CHUNK_SIZE'))

def _prioritize_cameras(tweets, docs):
	'''
	Prioritize given camera documents based off of mentions in given Tweet text.

	Args:
	`tweets`: [list(str)] The list of Tweet texts
	`docs`: [list(dict)] The list of raw "camera" documents

	Returns:
	An iterable where the first item is a list of all "camera" documents whose "title" values were found in at least one of the given Tweet texts and the second item is a list of all of the "camera" documents whose titles were not found in any of the given Tweet texts.
	'''
	high = []
	low = []
	# Iterate over documents
	for doc in docs:
		# Iterate over tweets
		for tweet in tweets:
			# If a camera was mentioned in a tweet, designate it as a high-priority camera
			if doc['title'].lower() in tweet.lower() or doc['title'].lower().replace(' ','') in tweet.lower():
				high.append(doc)
			else:
				low.append(doc)
	return high, low

def fetch_tweets(**kwargs):
	'''
	Fetch the last ten Tweets that include the query string "@alertwildfire" and get/chunk all "camera" documents in anticipation of scraping if at least one new Tweet is observed.
	'''
	from searchtweets import load_credentials, gen_request_parameters, collect_results

	# Load credentials from environment vars
	search_args = load_credentials(None)
	# Craft query
	query = gen_request_parameters('@alertwildfire', granularity=None, results_per_call=10)
	# Collect tweet results
	tweets = collect_results(query, max_tweets=10, result_stream_args=search_args)[0]['data']
	# List of new tweets
	new_tweets = []
	# Check for new tweets and insert into database
	for tweet in tweets:
		upsert_resp = mdb.insert_new_tweet(tweet['id'], tweet['text'])
		if upsert_resp:
			new_tweets.append(tweet['text'])
	# If a new tweet was found, kick off alertwildfire classic camera image scraping tasks
	if len(new_tweets)>0:
		# Get current epoch time
		epoch = int(time.time())
		# Retrieve all links from 'cameras' collection in database
		docs = mdb.get_docs('alertwildfire-cameras')
		# Prioritize cameras that were mentioned in new tweets
		high, low = _prioritize_cameras(new_tweets, docs)
		random.shuffle(low)
		# Combine prioritized documents
		sorted_docs = high + low
		# Chunk the docs into n-long groups
		chunked_docs = [sorted_docs[i:i+n] for i in range(0, len(sorted_docs), n)]
		# Push epoch timestamp and chunk of docs to xcom
		for i in range(len(chunked_docs)):
			kwargs['ti'].xcom_push(key=f'classic-chunk-{i}', value=[epoch, chunked_docs[i]])
		return True
	else:
		return False

def branch(**kwargs):
	'''
	Decipher whether or not scraping needs to occur.
	'''
	continue_to_scrape = kwargs['ti'].xcom_pull('fetch-tweets')
	if continue_to_scrape:
		return 'scrape-dummy'
	else:
		return 'noscrape-dummy'

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
	scraper_id = kwargs['ti'].task_id.split('-')[-1]
	message = kwargs['ti'].xcom_pull(key=f'classic-chunk-{scraper_id}', task_ids='fetch-tweets')
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
					task_id = f'redis-publish-{scraper_id}.{task_num}',
					channel = 'redis-detection-channel',
					redis_conn_id = 'redis-default',
					message = f'''{id}   {base64.b64encode(r.content).decode('utf-8')}''',
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
	# Return
	return {"success": success, "failure": failure}

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
	max_active_runs = 1,
	catchup = False,
	schedule_interval = '*/2 * * * *', # Every two minutes
	dagrun_timeout=datetime.timedelta(days=1) # 24 hour timeout
)
opr_fetch_tweets = PythonOperator(
	task_id = 'fetch-tweets',
	python_callable = fetch_tweets,
	provide_context = True,
	dag = DAG
)
branch = BranchPythonOperator(
	task_id = 'branch',
	provide_context = True,
	python_callable = branch,
	dag = DAG
)
opr_scrape_dummy = DummyOperator(
	task_id = 'scrape-dummy',
	dag = DAG
)
opr_noscrape_dummy = DummyOperator(
	task_id = 'noscrape-dummy',
	dag = DAG
)
# Decipher number of tasks to include in group
cam_count = mdb.get_count('alertwildfire-cameras')
task_count = cam_count//n
if cam_count%n != 0:
	task_count += 1
group = []
# Create task group
with TaskGroup(
	group_id = 'scraper-group',
	dag = DAG) as scraper_group:
	for chunk in range(task_count):
		group.append(
			PythonOperator(
				task_id = f'scrape-classic-{chunk}',
				python_callable = scrape_classic,
				provide_context = True,
				dag = DAG
			)
		)

# ========================================================================

opr_fetch_tweets >> branch >> opr_noscrape_dummy
opr_fetch_tweets >> branch >> opr_scrape_dummy >> scraper_group
