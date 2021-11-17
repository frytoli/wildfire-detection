#!/usr/bin/python3

'''
One-time enumerator to scrape and save camera URLs to database
Last updated: 2021-11-16
'''

from pymongo import MongoClient
from requests import exceptions
from pymongo import exceptions
import datetime
import random
import time
import uuid
import os

class arangodb():
	def __init__(self):
		DB_HOST = os.getenv('DB_HOST')
		DB_PORT = os.getenv('DB_PORT')
		DB_USER = os.getenv('DB_USER')
		DB_PASS = os.getenv('DB_PASS')
		DB_NAME = os.getenv('DB_NAME')
		# Attempt to establish a connection
		self.db = None
		client = MongoClient(f'mongodb://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}')
		self.db = client[DB_NAME]

	def insert_camera(self, axis, region, url, title, request_time):
		collection = self.db['alertwildfire-cameras']
		doc = {
			'id': str(uuid.uuid4()),
			'axis':axis,
			'region': region,
			'url':url,
			'title': title,
			'request_time': request_time,
			'request_time_delta': 15,
			'time_units': 'seconds',
			'timestamp':datetime.datetime.utcnow().isoformat()
		}
		collection.insert_one(doc)

def enumerate():
	# Initialize db object
	adb = arangodb()
	# Set root url and regions
	root_url = 'http://www.alertwildfire.org/'
	regions = [
		'oregon',
		'shastamodoc',
		'northcoast',
		'blmnv',
		'utah',
		'tahoe',
		'northbay',
		'southeastbay',
		'sierra',
		'centralcoast',
		'orangecoca',
		'inlandempire',
		'sdge'
	]
	random.shuffle(regions)
	# Initialize HTML session
	session = HTMLSession()
	# Iterate over regions and collect/save camera urls
	for region in regions:
		print(f'[-] Retrieving cameras in {region}')
		region_url = f'{root_url}{region}/index.html'
		# Request page and render JS
		r = session.get(region_url)
		r.html.render()
		# Find camera thumbnails
		thumbnails = r.html.find('.thumbNail')
		# Iterate over thumbnails, craft urls, and save to database
		for thumb in thumbnails:
			# Find thumbnail image
			img = thumb.find('img', first=True)
			# Extract camera id
			id = img.attrs['id']
			# Find image source
			src = f'''http:{img.attrs['data-src']}'''
			src = src.replace('thumb','full').strip()
			# Parse out base request time
			request_time = int(src.split('=')[-1])
			src = src.replace(str(request_time), '')
			# Find thumbnail caption
			p = thumb.find('p', first=True)
			# Get camera title
			title = p.text
			# Save to database
			adb.insert_camera(id, region, src, title, request_time)
			print(f'  [+] Camera {id} saved')
		time.sleep(random.randint(4,20))
	session.close()

if __name__ == '__main__':
	enumerate()
