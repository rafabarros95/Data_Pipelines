"""Postgres connection helper.

- Loads credentials from environment variables
- Exposes `connect_to_db()` for reuse in load scripts
"""
import os
from pathlib import Path

import psycopg

def load_env() -> None:
    """Load KEY=VALUE from Data_Pipelines/.env into os.environ if not already set."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
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