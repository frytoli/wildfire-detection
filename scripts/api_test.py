#!/usr/bin/python3

from requests_html import HTMLSession
import base64

if __name__ == '__main__':
    session = HTMLSession()
    r = session.post(
        'http://127.0.0.1:8000/detect',
        json = {
            'image': base64.b64encode(open('test_images/frame0.jpg', 'rb').read()).decode('utf-8')
        }
    )
    print(r)
