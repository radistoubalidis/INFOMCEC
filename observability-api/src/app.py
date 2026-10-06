import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query
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
    upper_threshold = payload[0][0]["temperature_range"]["upper_threshold"]
    lower_threshold = payload[0][0]["temperature_range"]["lower_threshold"]

    started_timestamp = request.state.conn.execute(
        "SELECT timestamp FROM events WHERE event_name='experiment_started' AND experiment_id=%s ORDER BY timestamp ASC LIMIT 1", 
        (experiment_id,),).fetchone()[0]

    data = request.state.conn.execute(
        "SELECT min(timestamp), avg(temperature) FROM events WHERE event_name='sensor_temperature_measured' AND experiment_id = %s AND timestamp > %s GROUP BY measurement_id HAVING avg(temperature) < %s OR avg(temperature) > %s ORDER BY min(timestamp)",
    (experiment_id, started_timestamp, lower_threshold, upper_threshold),).fetchall()
    return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]


@app.get("/temperature")
def temperature(request: Request, experiment_id: str=Query(alias="experiment-id"), start_time: float=Query(alias="start-time"), end_time: float=Query(alias="end-time")):
    experiment_started = request.state.conn.execute(
        "SELECT timestamp FROM events WHERe event_name='experiment_started' AND experiment_id=%s ORDER by timestamp ASC LIMIT 1",
       (experiment_id,),).fetchone()[0]

    data = request.state.conn.execute(
        "SELECT min(timestamp), avg(temperature) FROM events WHERE experiment_id=%s AND event_name='sensor_temperature_measured' AND timestamp > %s AND timestamp BETWEEN to_timestamp(%s) AND to_timestamp(%s) GROUP BY measurement_id ORDER BY min(timestamp)",
        (experiment_id, experiment_started, start_time, end_time),).fetchall()
    return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]
