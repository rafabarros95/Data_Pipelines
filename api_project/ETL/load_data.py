import os

from api_project.ETL.postgresSQL_conn import connect_to_db

conn = connect_to_db()
# grab the database name from the env
dbname = os.getenv("POSTGRES_DB")

with connect_to_db() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database();")
        dbname = cur.fetchone()[0]
    print(f"Connected to database: {dbname}")
