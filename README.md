# Wildfire Detection

[Building and Deploying](#building-and-deploying)

[Airflow](#airflow)

[Detection API](#detection-api)

[Helper Scripts](#helper-scripts)

## Building and Deploying
1. Copy ```docker-compose.yml.changeme``` to ```docker-compose.yml``` and edit the environment variables accordingly.

2. Build and bring project up:
```bash
docker-compose build && docker-compose up -d
```

3. Port forward:
```bash
ssh -N -L 8080:127.0.0.1:8888 -L 5555:127.0.0.1:5555 -L 1905:127.0.0.1:1905 wildfire-detection
```

4. Add the "redis-default" Airflow connection to the Redis container and Airflow variables either individually or with a json file like ```secrets.json.changeme``` via the Airflow webserver which should be accessible on 127.0.0.1:8080 in a browser.

## Airflow
stuff

### ALERTWildfire Scraper DAG
stuff

### Model Detection DAG
stuff

### Webserver
stuff

### Workers
stuff

## Detection API
stuff

## Helper Scripts
A number of scripts have been included in ```scripts/``` for auxiliary tasks related to the Airflow and Detection API services.

### api_test.py
stuff

### classic_enumerator.py
stuff

### get_output_images.py
stuff

## To Do
[x] a thing
[ ] another thing
