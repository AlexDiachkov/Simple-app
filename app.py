import uvicorn
from fastapi import FastAPI, Depends
import yaml
import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

app = FastAPI()


def get_db() -> cursor:
    # .env
    with psycopg2.connect(
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            database=os.environ["POSTGRES_DATABASE"],
            cursor_factory=RealDictCursor,
    ) as conn:
        return conn


def config():
    with open("params.yaml", "r") as f:
        return yaml.safe_load(f)


@app.get("/users")
def get_all_users(limit: int = 10, conn: connection = Depends(get_db)):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM "user"
            LIMIT %(limit_users)s
            """, {"limit_users": limit}
        )
        return cursor.fetchall()


@app.get("/user/feed")
def get_user_feed(user_id: int, limit: int = 10, conn: connection = Depends(get_db), config: dict = Depends(config)):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM feed_action
            WHERE user_id = %(user_id)s
                AND time >= %(start_date)s
            LIMIT %(limit)s
            """, {"user_id": user_id, "limit": limit, "start_date": config["feed_start_date"]}
        )
        return cursor.fetchall()


if __name__ == '__main__':
    load_dotenv()
    uvicorn.run(app)
