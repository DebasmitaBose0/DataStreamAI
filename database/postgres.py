"""
DataStream AI - Relational Database Connector & Unified Layer
Handles PostgreSQL with an automatic zero-config SQLite local fallback.
"""

import os
import sqlite3
import logging
from pathlib import Path
from typing import Optional, Tuple, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("datastream.database")
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = BASE_DIR / "data" / "datastream.db"
SCHEMA_SQL_PATH = BASE_DIR / "database" / "schema.sql"


class DatabaseManager:
    _instance: Optional["DatabaseManager"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._engine = None
            cls._instance._db_type = "uninitialized"
            cls._instance._init_connection()
        return cls._instance

    def _init_connection(self):
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_db = os.getenv("POSTGRES_DB", "datastream_db")
        pg_user = os.getenv("POSTGRES_USER", "postgres")
        pg_pass = os.getenv("POSTGRES_PASSWORD", "postgres")

        pg_uri = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

        # Attempt PostgreSQL connection with quick timeout
        try:
            temp_engine = create_engine(
                pg_uri,
                connect_args={"connect_timeout": 2},
                pool_pre_ping=True
            )
            with temp_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._engine = temp_engine
            self._db_type = "PostgreSQL"
            logger.info("Connected successfully to PostgreSQL database: %s", pg_db)
        except Exception as e:
            logger.info("PostgreSQL unavailable (%s). Falling back gracefully to SQLite: %s", e, SQLITE_DB_PATH)
            SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            self._engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}")
            self._db_type = "SQLite (Local Fallback)"

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._init_connection()
        return self._engine

    @property
    def db_type(self) -> str:
        return self._db_type

    def init_schema(self, force_reset: bool = False):
        """Initializes tables from schema.sql"""
        if not SCHEMA_SQL_PATH.exists():
            logger.error("schema.sql not found at %s", SCHEMA_SQL_PATH)
            return

        with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        statements = [stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()]

        with self.engine.begin() as conn:
            if force_reset:
                for table in ["watch_history", "events", "subscriptions", "content", "users"]:
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                    except Exception:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table};"))

            for stmt in statements:
                if stmt:
                    conn.execute(text(stmt))
        logger.info("Database schema initialized successfully for %s", self.db_type)

    def query(self, sql_query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Executes a SQL query and returns a pandas DataFrame."""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(text(sql_query), conn, params=params)
                return df
        except Exception as e:
            logger.error("Query failed: %s | Error: %s", sql_query, e)
            return pd.DataFrame()

    def execute_raw(self, sql_statement: str, params: Optional[dict] = None) -> int:
        """Executes INSERT/UPDATE/DELETE statement."""
        with self.engine.begin() as conn:
            result = conn.execute(text(sql_statement), params or {})
            return result.rowcount

    def get_table_counts(self) -> dict:
        """Returns row counts for all platform tables."""
        counts = {}
        for table in ["users", "content", "events", "subscriptions", "watch_history"]:
            try:
                res = self.query(f"SELECT COUNT(*) AS cnt FROM {table}")
                counts[table] = int(res["cnt"].iloc[0]) if not res.empty else 0
            except Exception:
                counts[table] = 0
        return counts


# Singleton helper instances
db_manager = DatabaseManager()

def get_db():
    return db_manager
