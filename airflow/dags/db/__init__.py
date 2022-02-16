#!/usr/bin/env python3

from pymongo import MongoClient, errors
from requests import exceptions
import datetime
import time

class mongo():
	def __init__(self, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME):
		# Attempt to establish a connection
		self.db = None
		while not self.db:
			try:
				client = MongoClient(f'mongodb://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}')
				self.db = client[DB_NAME]
			except (exceptions.ConnectionError, errors.ConnectionFailure) as e:
				print(f'[!] Failed to establish a connection: {e}\n  [-] Trying again in 5 seconds')
				time.sleep(5)

	def get_docs(self, collection, filter={}):
		'''
			Fetch documents from database with optional filter
		'''
		collection = self.db[collection]
		return list(collection.find(filter))

	def insert_new_tweet(self, tweetid, text):
		'''
			Insert a new tweet document into the collection and ignore previously-seen tweets
		'''
		collection = self.db['tweets']
		result = collection.update_one(
			{ 'tweetid': tweetid, 'text': text },
			{ '$set': { 'tweetid': tweetid } },
			upsert = True
		)
		return result.upserted_id # ID of new record or None if record already exists

	def get_count(self, collection):
		collection = self.db[collection]
		return int(collection.count())

	def get_all_regions(self):
		collection = self.db['alertwildfire-cameras']
		return list(collection.find().distinct('region'))

	def insert_detection(self, id):
		doc = {
			'id': id,
			'status': 'recieved',
			'recieved_timestamp': datetime.datetime.utcnow().isoformat(),
			'processed_timestamp': None,
			'processed_time': None,
			'fire': None,
			'image': None
		}
		# Insert
		collection = self.db['detections']
		result = collection.insert_one(doc)

	def update_detection(self, id, ptimestamp, ftime, stime, detected, image=None):
		new_values = {
			'status': 'detected',
			'detection_timestamp': ptimestamp.isoformat(),
			'fire_detection_time': ftime,
			'smoke_detection_time': stime,
			'detected': detected,
			'image': None
		}
		if image:
			new_values['image'] = image
		# Update
		collection = self.db['detections']
		result = collection.update_one(
			{ 'id': id },
			{ '$set': new_values }
		)
		return result.upserted_id
