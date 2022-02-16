#!/usr/bin/env python3

from pydantic import BaseModel
from fastapi import FastAPI
from typing import Optional
import redis
import os

app = FastAPI()

r = redis.Redis(host=os.getenv('BACKEND_HOST'), port=os.getenv('BACKEND_PORT'))

class Image(BaseModel):
	id: str
	image: str

class AW_Regions(BaseModel):
	regions: list

@app.post('/detect/', status_code=200)
def post_detect(image: Image):
	r.publish('redis-detection-channel', f'{image.id}   {image.image}')
	return image.id

@app.post('/scrape-aw-region/', status_code=200)
def post_scrape_aw(regions: AW_Regions):
	r.publish('redis-alertwildfire-channel', f'{','.join(regions.regions)}')
	return True
