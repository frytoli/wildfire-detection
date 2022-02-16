#!/usr/bin/python3

'''
Send either a test image to the Detection API or a list of AlertWildfire region(s) to trigger the scraping of all the cameras in the region(s)
'''

import requests
import base64
import os

def post_image(id, region, image_path):
	r = requests.post(
		'http://127.0.0.1:1905/detect',
		json = {
			'id': id,
			'region': region,
			'image': base64.b64encode(open(image_path, 'rb').read()).decode('utf-8')
		}
	)
	return r

def post_regions(regions):
	r = requests.post(
		'http://127.0.0.1:1905/scrape-aw-region',
		json = {
			'regions': regions
		}
	)
	return r

if __name__ == '__main__':
	# Test detection endpoint
	post_image('test', 'test', os.path.join(os.getcwd(), 'test_images', 'frame0.jpg'))
	# Test scrape regions endpoint
	post_regions(['orangecoca'])
