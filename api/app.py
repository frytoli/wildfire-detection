#!/usr/bin/env python3

from typing import Optional
from fastapi import FastAPI
import redis
import os

app = FastAPI()

@app.get('/')
def get_root():
    return {'Hello': 'World'}

@app.put('/detect')
def put_detect(image: bytes):
    return {'image': image}
