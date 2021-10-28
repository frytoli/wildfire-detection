#!/usr/bin/env python3

# Airflow
from airflow.contrib.operators.redis_publish_operator import RedisPublishOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG

# Other
from searchtweets import load_credentials, gen_request_parameters, collect_results
from requests_html import HTMLSession, AsyncHTMLSession
import datetime
import asyncio
import logging
import random
import time
#import db
import re

def prioritize_cameras(tweets, docs):
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

def fetch_tweets():
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
		upsert_resp = adb.insert_new_tweet(tweet['id'], tweet['text'])[0]
		if upsert_resp['type'] == 'insert':
			new_tweets.append(tweet['text'])
	# If a new tweet was found, kick off alertwildfire classic camera image scraping tasks
	if len(new_tweets)>0:
		# Retrieve all links from 'cameras' collection in database
		docs = adb.get_docs('cameras')
		# Prioritize cameras that were mentioned in new tweets
		high, low = prioritize(new_tweets, docs)
		random.shuffle(low)
		# Combine prioritized documents
		sorted_docs = high + low
		# Chunk the docs into n-long groups
		chunked_docs = [sorted_docs[i:i+n] for i in range(0, len(sorted_docs), n)]
		# Return
		return chunked_docs

def get_proxies():
	'''
		Fetch a free list of proxies from Proxyscrape.com

		Returns:
			(list(str)) A list of valid proxy:port pair strings
	'''
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

def make_afunc(asession, axis, url, proxy, headers={}, render=False):
	'''
		Dynamically build an asynchronous function at runtime for use by requests-html's AsyncHTMLSession's run() method

		Args:
			asession: (AsyncHTMLSession) the AsyncHTMLSession object
			axis: (str) the document's axis
			url: (str) the docuemnt's url to the camera
			proxy: (str) a proxy:port pair
			headers: (dict) (optional) dictionary of desired request headers
			render: (bool) (optional) indicator to render (or not) the javascript of the request's returned html

		Returns:
			asynchronous function
				Returns:
					(requests-html response object)
					(str) the document's axis
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
		return r, axis, url, proxy
	return _afunction

def scrape_classic(**kwargs):
	'''
		Asynchronous scrape and save images from a group of camera urls

		Args:
			saveto_dir: (str) path to the directory where the images are to be saved
			docs: (list(dic())) a list of json documents with keys "axis" and "url"
			timeout: (int) (optional) number of seconds until timeout; this defaults to 50 minutes, 10 minutes less than RabbitMQ's timeout

		Returns:
			dictionary object of {"success": int() successful job count, "failure":int() failed job count}
	'''
	# Setup timer
	start = time.time()
	elapsed = time.time()-start
	# Instantiate success and failure count objects
	success = 0
	failure = len(docs)
	# Ensure that timeout var is correct type
	if not (isinstance(timeout, int) or isinstance(timeout, int)):
		print('[!] Provided value for timeout is not int or float. Defaulting to timeout of 3000 seconds.')
		timeout = 3000
	# Initialize drive object
	gd = drive.gdrive()
	# Fetch proxies
	proxies = get_proxies()
	# If request to proxyscrape was bad, sleep and try again
	if len(proxies) < 1:
		# Sleep randomly
		time.sleep(random.randint(13,30))
		# Try again
		proxies = get_proxies()
	# Initialize step vars
	active = {doc['axis']: {'url': doc['url'], 'proxy': None, 'step':1, 'tries':0} for doc in docs}
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
		# Shuffle proxies
		random.shuffle(proxies)
		# Craft dynamic arguments for async session
		aargs = []
		for axis in active:
			camera = active[axis]
			if camera['step'] == 1: # Step 1
				proxy = random.choice(proxies)
				# Make async function for initial page request
				aargs.append(make_afunc(asession, axis, camera['url'], proxy, render=True))
			elif camera['step'] == 2: # Step 2
				# Make async function for image page request
				aargs.append(make_afunc(asession, axis, camera['url'], camera['proxy'], headers={'Referer': 'http://www.alertwildfire.org/'}))
		# Make async requests and pair with axis
		results = asession.run(*aargs)
		# Iterate over results and complete tasks per step if previous request was successful
		for result in results:
			# Parse result
			r = result[0]
			axis = result[1]
			url = result[2]
			proxy = result[3]
			step = active[axis]['step']
			if r and r.status_code in [200, 301, 302, 303, 307]:
				print(f'[-] Good response from {url}')
				# Step 1
				if step == 1:
					# Find direct image url
					img = r.html.find('.leaflet-image-layer', first=True).attrs['src']
					src = f'http:{img}'
					# Update record
					active[axis]['url'] = src
					active[axis]['proxy'] = proxy
					active[axis]['step'] = 2
					# Close request object
					r.close()
				# Step 2
				elif step == 2:
					# Publish image as a message to the Redis fire-detection queue with a RedisPublishOperator
					opr_redis_publish = RedisPublishOperator(
						task_id = f'redis-publish-{task_num}',
				        channel = Variable.get('redis-detection-channel'),
				        redis_conn_id = 'redis_default',
						message = r.content,
						dag = DAG
					).execute(context=kwargs)
					task_num += 1
					# Update record, aka remove it from the list of active tasks
					del active[axis]
					# Close request object
					r.close()
					# Increment success and decrement failure
					success += 1
					failure -= 1
			# If not successful
			else:
				print(f'[-] Bad response from {url}')
				# Step 1
				if step == 1:
					# Select a new proxy and update the record
					active[axis]['proxy'] = random.choice(proxies)
					# Close request object
					if r:
						r.close()
				# Step 2
				elif step == 2:
					# Select a new proxy and update the record
					active[axis]['proxy'] = random.choice(proxies)
					# Increment number of tries
					active[axis]['tries'] += 1
					# If five tries have been made, demote the step back to step 1
					if active[axis]['tries'] >= 5:
						active[axis]['tries'] = 0
						active[axis]['step'] = 1
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
	'retry_delay': datetime.timedelta(seconds=5),
	'concurrency': 2
}

# Build the DAG
DAG = DAG(
	dag_id = 'alertwildfire-classic-scraper',
	description = 'ALERTWildfire classic camera image scraper invoked upon observation of new tweets @ALERTWildfire',
	default_args = default_args,
	catchup = False,
	schedule_interval = '*/2 * * * *', # Every two minutes
	dagrun_timeout=datetime.timedelta(days=1) # 24 hour timeout
)
opr_fetch_tweets = PythonOperator(
	task_id = 'fetch-tweets',
	python_callable = temp_fetch_tweets,
	provide_context = True, # Unsure about this for first task
	dag = DAG
)
opr_scrape_classic = PythonOperator(
	task_id = 'scrape-classic',
	python_callable = temp_scrape_classic,
	provide_context = True,
	dag = DAG
)

# ========================================================================

opr_fetch_tweets >> opr_scrape_classic
