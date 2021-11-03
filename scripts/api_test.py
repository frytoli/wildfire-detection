#!/usr/bin/python3

from requests_html import HTMLSession

if __name__ == '__main__':
    session = HTMLSession()
    r = session.post(
        'http://127.0.0.1:80/detect',
        data = {
            'image': open('test.jpg', 'wb').read()
        }
    )
    print(r)
