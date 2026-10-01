# Setup Guide

1. Create a `.env` file in the root folder and fill in the values :
```
TOPIC=
SSL_CA_LOCATION=
BOOTSTRAP_SERVERS=
SSL_KEYSTORE_LOC=
SSL_KEYSTORE_PASSWORD=
BROKER=

# db
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_PORT=
```

2. Add all the files from the group6 folder we were given inside a folder called `auth` in the root folder
3. Make sure you have docker-compose installed on your machine (it's included in Docker Desktop)
4. With cwd the root folder run `docker-compose up -d`. This pulls/creates the docker images runs the experiments and saves it to a postgres db 

## Observability-api 
The folder contains a FastAPI app that is already connected to the postgres db 
Docs: https://fastapi.tiangolo.com/