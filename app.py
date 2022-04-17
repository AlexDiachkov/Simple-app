import uvicorn
from fastapi import FastAPI, Depends
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

app = FastAPI()


def get_db():
    return psycopg2.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DATABASE"]
    )


@app.get("/users")
def get_all_users(limit: int = 10, db=Depends(get_db)):
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(f"""
        SELECT *
        FROM "user"
        LIMIT {limit}
        """)
        return cursor.fetchall()


if __name__ == '__main__':
    load_dotenv()
    uvicorn.run(app)
