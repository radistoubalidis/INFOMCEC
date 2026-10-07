import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query, HTTPException
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ["DATABASE_URL"]

"""
You can check localhost:8000/docs to see and run the API endpoints on the browser
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=True)
    yield
    app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def db_connection(request: Request, call_next):
    with request.app.state.pool.connection() as conn:
        request.state.conn = conn
        return await call_next(request)


@app.get("/")
def hello():
    return {"message": "Hello, World!"}


"""
This is an example on how to run queries
"""
@app.get("/db")
def db_version(request: Request):
    row = request.state.conn.execute("SELECT version()").fetchone()
    return {"version": row[0]}

@app.get("/temperature/out-of-range")
def out_of_range(request: Request, experiment_id: str=Query(alias="experiment-id")):
    payload = request.state.conn.execute(
        "SELECT payload FROM events WHERE event_name='ExperimentConfig' AND experiment_id=%s ", 
        (experiment_id,),).fetchall()
    if not payload:
        raise HTTPException(status_code=404, detail=f"ExperimentConfig not found for exp {experiment_id}")
    upper_threshold = payload[0][0]["temperature_range"]["upper_threshold"]
    lower_threshold = payload[0][0]["temperature_range"]["lower_threshold"]

    started_timestamp = request.state.conn.execute(
        "SELECT min(timestamp) FROM events WHERE event_name='experiment_started' AND experiment_id=%s", 
        (experiment_id,),).fetchone()[0]
    if started_timestamp is None:
        raise HTTPException(status_code=404, detail=f"experiment_started not found for exp {experiment_id}")

    data = request.state.conn.execute(
        "SELECT timestamp, temperature FROM temperature_averages WHERE experiment_id = %s AND timestamp > %s AND (temperature < %s OR temperature > %s) ORDER BY timestamp",
    (experiment_id, started_timestamp, lower_threshold, upper_threshold),).fetchall()
    return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]


@app.get("/temperature")
def temperature(request: Request, experiment_id: str=Query(alias="experiment-id"), start_time: float=Query(alias="start-time"), end_time: float=Query(alias="end-time")):
    row = request.state.conn.execute(
        "SELECT min(timestamp) FROM events WHERE event_name='experiment_started' AND experiment_id=%s",
        (experiment_id,),).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"experiment_started not found for exp {experiment_id}")
    experiment_started = row[0] 

    data = request.state.conn.execute(
        "SELECT timestamp, temperature FROM temperature_averages WHERE experiment_id=%s AND timestamp > %s AND timestamp BETWEEN to_timestamp(%s) AND to_timestamp(%s) ORDER BY timestamp",
        (experiment_id, experiment_started, start_time, end_time),).fetchall()
    return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]
