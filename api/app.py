#!/usr/bin/env python3

from pydantic import BaseModel
from fastapi import FastAPI
from typing import Optional
import redis
import os

app = FastAPI()

r = redis.Redis(host=os.getenv('BACKEND_HOST'), port=os.getenv('BACKEND_PORT'))

class Image(BaseModel):
    image: str

@app.post('/detect/', status_code=200)
def post_detect(image: Image):
    r.publish('redis-detection-channel', image.image)
