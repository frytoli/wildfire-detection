#!/usr/bin/env python3

from pyArango.connection import *
from pyArango import theExceptions
from requests import exceptions
import datetime
import time

class arangodb():
	def __init__(self, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME):
		# Attempt to establish a connection
		self.db = None
		while not self.db:
			try:
				self.db = Connection(
					arangoURL='http://{}:{}'.format(DB_HOST, DB_PORT),
					username=DB_USER,
					password=DB_PASS
				)[DB_NAME]
			except (theExceptions.ConnectionError, exceptions.ConnectionError) as e:
				print(f'[!] Failed to establish a connection: {e}\n  [-] Trying again in 5 seconds')
				time.sleep(5)

	def get_docs(self, collection, tweetid=0):
		'''
			Fetch documents from database with optional time range filtering
		'''
		bindVars = {'@collection': collection}
		if tweetid:
			starttime = (datetime.datetime.utcnow() - datetime.timedelta(seconds=secsdelta)).isoformat
			aql = '''
				FOR doc IN @@collection
					FILTER doc.tweetid > @tweetid
					RETURN doc
			'''
			bindVars['tweetid'] = tweetid
		else:
			aql = '''
				FOR doc IN @@collection
					RETURN doc
			'''
		return list(self.db.AQLQuery(aql, bindVars=bindVars, rawResults=True))

	def insert_new_tweet(self, tweetid, text):
		'''
			Insert a new tweet document into the collection and ignore previously-seen tweets
		'''
		bindVars = {
			'tweetid': tweetid,
			'text': text,
			'timestamp': datetime.datetime.utcnow().isoformat()
		}
		aql = '''
			UPSERT { id: @tweetid }
			INSERT { id: @tweetid, text: @text, scrape_timestamp: @timestamp }
			UPDATE { } IN tweets
			OPTIONS { exclusive: true }
			RETURN { doc: NEW, type: OLD ? 'update' : 'insert' }
		'''
		return self.db.AQLQuery(aql, bindVars=bindVars, rawResults=True)

	def get_count(self, collection):
		binVars = {'@collection': collection}
		aql = '''RETURN LENGTH(@@collection)'''
		return self.db.AQLQuery(aql, bindVars=bindVars, rawResults=True)
