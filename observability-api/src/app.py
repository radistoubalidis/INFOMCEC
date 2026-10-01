import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
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