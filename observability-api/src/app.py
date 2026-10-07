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

@app.get("/")
def hello():
    return {"message": "Hello, World!"}


"""
This is an example on how to run queries
"""
@app.get("/db")
def db_version():
    with app.state.pool.connection() as conn:
        row = conn.execute("SELECT version()").fetchone()
        return {"version": row[0]}

@app.get("/temperature/out-of-range")
<<<<<<< Updated upstream
def out_of_range(request: Request, experiment_id: str=Query(alias="experiment-id")):
    experiment = request.state.conn.execute(
        "SELECT 1 FROM experiment_states WHERE experiment_id = %s",
        (experiment_id,),
    ).fetchone()
    if experiment is None:
        raise HTTPException(status_code=404, detail=f"Experiment not found: {experiment_id}")

    data = request.state.conn.execute(
        "SELECT timestamp, temperature FROM temperature_averages WHERE experiment_id = %s AND in_range = FALSE ORDER BY timestamp",
    (experiment_id,),).fetchall()
    return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]


@app.get("/temperature")
def temperature(request: Request, experiment_id: str=Query(alias="experiment-id"), start_time: float=Query(alias="start-time"), end_time: float=Query(alias="end-time")):
    experiment = request.state.conn.execute(
    "SELECT 1 FROM experiment_states WHERE experiment_id = %s",
    (experiment_id,),
    ).fetchone()
    if experiment is None:
        raise HTTPException(status_code=404, detail=f"Experiment not found: {experiment_id}")
    
    row = request.state.conn.execute(
        "SELECT min(timestamp) FROM events WHERE event_name='experiment_started' AND experiment_id=%s",
        (experiment_id,),).fetchone()
    if row is None or row[0] is None:
        raise HTTPException(status_code=404, detail=f"experiment_started not found for exp {experiment_id}")
    experiment_started = row[0] 

    data = request.state.conn.execute(
        "SELECT timestamp, temperature FROM temperature_averages WHERE experiment_id=%s AND timestamp > %s AND timestamp BETWEEN to_timestamp(%s) AND to_timestamp(%s) ORDER BY timestamp",
        (experiment_id, experiment_started, start_time, end_time),).fetchall()
    return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]
=======
def out_of_range(experiment_id: str=Query(alias="experiment-id")):
    with app.state.pool.connection() as conn:
        experiment = conn.execute(
            "SELECT 1 FROM experiment_states WHERE experiment_id = %s",
            (experiment_id,),
        ).fetchone()
        if experiment is None:
            raise HTTPException(status_code=404, detail=f"Experiment not found: {experiment_id}")

        data = conn.execute(
            "SELECT timestamp, temperature FROM temperature_averages WHERE experiment_id = %s AND in_range = FALSE ORDER BY timestamp",
        (experiment_id,),).fetchall()
        return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]


@app.get("/temperature")
def temperature(experiment_id: str=Query(alias="experiment-id"), start_time: float=Query(alias="start-time"), end_time: float=Query(alias="end-time")):
    with app.state.pool.connection() as conn:
        experiment = conn.execute(
        "SELECT 1 FROM experiment_states WHERE experiment_id = %s",
        (experiment_id,),
        ).fetchone()
        if experiment is None:
            raise HTTPException(status_code=404, detail=f"Experiment not found: {experiment_id}")
        
        row = conn.execute(
            "SELECT min(timestamp) FROM events WHERE event_name='experiment_started' AND experiment_id=%s",
            (experiment_id,),).fetchone()
        if row is None or row[0] is None:
            raise HTTPException(status_code=404, detail=f"experiment_started not found for exp {experiment_id}")
        experiment_started = row[0] 

        data = conn.execute(
            "SELECT timestamp, temperature FROM temperature_averages WHERE experiment_id=%s AND timestamp > %s AND timestamp BETWEEN to_timestamp(%s) AND to_timestamp(%s) ORDER BY timestamp",
            (experiment_id, experiment_started, start_time, end_time),).fetchall()
        return [{"timestamp": timestamp.timestamp(), "temperature": temperature} for timestamp, temperature in data]
>>>>>>> Stashed changes
