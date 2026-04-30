"""Postgres connection helper.

- Loads credentials from environment variables
- Exposes `connect_to_db()` for reuse in load scripts
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg

def load_env() -> None:
    """Load KEY=VALUE from Data_Pipelines/.env into os.environ if not already set."""
    env_path = Path(__file__).resolve().parents[2] / ".env" # read the .env file to get the credentials for the database connection
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        if not line.strip() or line.strip().startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if sep and key and value and key not in os.environ:
            os.environ[key] = value

def connect_to_db() -> psycopg.Connection:
    """Create and return a Postgres connection using env vars.

    Required env vars:
      POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    Optional:
      POSTGRES_PORT (defaults to 5432)
    """
    load_env()

    try:
        host = os.environ["POSTGRES_HOST"]
        dbname = os.environ["POSTGRES_DB"]
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
    except KeyError as e:
        missing = e.args[0]
        raise RuntimeError(
            f"Missing environment variable: {missing}. "
            f"Set it in your OS/PyCharm run config or in the project root .env file."
        ) from e

    port = int(os.environ.get("POSTGRES_PORT", "5432"))

    return psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )

# local testing
if __name__ == "__main__":

    load_env()
    print(f"Connecting to Postgres database at {os.environ['POSTGRES_DB']}...")
    print(f"Connecting to Postgres user at {os.environ['POSTGRES_USER']}...")
    print(f"Connecting to Postgres host at {os.environ['POSTGRES_HOST']}...")

    try:
        with connect_to_db() as conn:
            print("Successfully connected to Postgres database.")
    except Exception as e:
        print(f"Failed to connect to Postgres database: {e}")