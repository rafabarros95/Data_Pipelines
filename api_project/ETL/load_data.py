"""
We grab the cities_df_cleaned and save into our PostgresSQL Database created and it's running on Docker Container
"""

import os
import pandas as pd
import psycopg
import logging

from api_project.ETL.postgresSQL_conn import connect_to_db

logger = logging.getLogger(__name__)
# Load the cleaned dataframe from the CSV file
csv_path = os.path.join(os.path.dirname(__file__), "..", "processed_data", "cities_df_cleaned.csv")
cities_df = pd.read_csv(csv_path)

# Connect to the database
conn = connect_to_db()

class Loader:
    def __init__(self, df: pd.DataFrame, connection):
        self.cities_df = df
        self.conn = connection

    def load_data(self):
        columns = ["country", "province", "date", "name", "fips", "lat", "long",
                   "confirmed", "deaths", "confirmed_diff", "deaths_diff"]
        rows = [tuple(row) for row in self.cities_df[columns].itertuples(index=False)]
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(columns)

        query = f"INSERT INTO usa_covid_cases ({col_names}) VALUES ({placeholders})"

        with self.conn.cursor() as cur:
            cur.executemany(query, rows)
        self.conn.commit()
        logger.info("Data loaded successfully!")

if __name__ == "__main__":
    conn = connect_to_db()
    loader = Loader(df=cities_df, connection=conn)
    loader.load_data()
    conn.close()

