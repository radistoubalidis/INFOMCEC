import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Response
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ["DATABASE_URL"]

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=True)
    yield
    app.state.pool.close()


app = FastAPI(lifespan=lifespan)

@app.get("/")
def hello():
    return {"message": "Hello, World!"}


#This is an example on how to run queries
@app.get("/db")
def db_version():
    with app.state.pool.connection() as conn:
        row = conn.execute("SELECT version()").fetchone()
        return {"version": row[0]}

@app.get("/temperature/out-of-range")
def out_of_range(experiment_id: str=Query(alias="experiment-id")):
    with app.state.pool.connection() as conn:
        row = conn.execute(
            "SELECT (SELECT 1 FROM experiment_states WHERE experiment_id = %s), coalesce(json_agg(json_build_object('timestamp', extract(epoch FROM timestamp), 'temperature', temperature) ORDER BY timestamp), '[]')::text FROM temperature_averages WHERE experiment_id = %s AND in_range = FALSE",
            (experiment_id, experiment_id),
        ).fetchone()
    if row[0] is None:
        raise HTTPException(status_code=404, detail=f"Experiment not found: {experiment_id}")
    return Response(content=row[1], media_type="application/json")

@app.get("/temperature")
def temperature(experiment_id: str=Query(alias="experiment-id"), start_time: float=Query(alias="start-time"), end_time: float=Query(alias="end-time")):
    with app.state.pool.connection() as conn:
        row = conn.execute(
            "SELECT s.started_at, (SELECT coalesce(json_agg(json_build_object('timestamp', extract(epoch FROM t.timestamp), 'temperature', t.temperature) ORDER BY t.timestamp), '[]')::text FROM temperature_averages t WHERE t.experiment_id = s.experiment_id AND t.timestamp > s.started_at AND t.timestamp BETWEEN to_timestamp(%s) AND to_timestamp(%s)) FROM experiment_states s WHERE s.experiment_id = %s",
            (start_time, end_time, experiment_id),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Experiment not found: {experiment_id}")
    if row[0] is None:
        raise HTTPException(status_code=404, detail=f"experiment_started not found for exp {experiment_id}")
    return Response(content=row[1], media_type="application/json")
