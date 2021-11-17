#!/usr/bin/python3

'''
Send a test image to the Detection API
Last updated: 2021-11-16
'''

import requests
import base64

if __name__ == '__main__':
	r = requests.post(
		'http://127.0.0.1:1905/detect',
		json = {
			'id': 'test',
			'image': base64.b64encode(open('test_images/frame0.jpg', 'rb').read()).decode('utf-8')
		}
	)
	print(r)
