
from contextlib import contextmanager
import psycopg2

@contextmanager
def get_conn():
    conn = psycopg2.connect(
        dbname="ifx",
        user="ifx",
        password="ifx",
        host="localhost",
        port=5432
    )
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def executescript(sql_script: str):
    with get_conn() as conn:
        cur = conn.cursor()
        for stmt in sql_script.split(';'):
            if stmt.strip():
                cur.execute(stmt)
