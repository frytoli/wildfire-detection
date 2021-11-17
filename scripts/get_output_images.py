#!/usr/bin/python3

'''
Script to pull down, b64 decode, and write output images to a file
Last updated: 2021-11-16

To Run:
python get_output_image.py $LIMIT
  where $LIMIT is the integer number of images to return and write
'''

from pymongo import MongoClient
import base64
import json
import sys
import os

def main(limit=10):
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_NAME = os.getenv('DB_NAME')

    client = MongoClient(f'mongodb://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}')

    # Create database and collections
    db = client['early-fire-detection']
    collection = db['detections']

    docs = collection.find({'image': {'$ne': None}}).limit(limit)
    for doc in docs:
        print(f'''[+] Writing {doc['id']}.jpg''')
        with open(f'''output/{doc['id']}.jpg''', 'wb') as outfile:
            outfile.write(base64.b64decode(doc['image']))

if __name__ == '__main__':
    if len(sys.argv) > 1:
	    main(limit=int(sys.argv[1]))
    else:
        main()
