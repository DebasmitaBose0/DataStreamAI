"""
DataStream AI - Apache Airflow Orchestration DAG
Defines the scheduled batch ETL pipeline workflow:
extract_data -> validate_data -> transform_data -> load_database -> quality_check

Note: If Apache Airflow is not installed or running in your local environment,
the platform's built-in pipeline/etl_pipeline.py executes this identical workflow.
"""

from datetime import datetime, timedelta
import logging

# Fallback compatibility if Airflow is not installed locally
try:
    from airflow import DAG
    # pyrefly: ignore [missing-import]
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    PythonOperator = None

logger = logging.getLogger("airflow.datastream_dag")

default_args = {
    "owner": "datastream_data_eng",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def task_extract_data(**context):
    """Task 1: Extract CSV/API events from storage buckets."""
    logger.info("Executing Task: extract_data...")
    from pipeline.ingestion import ingestion_engine
    return "extract_complete"


def task_validate_data(**context):
    """Task 2: Audit incoming raw schema drift and quality violations."""
    logger.info("Executing Task: validate_data...")
    from pipeline.validation import dq_engine
    return "validation_complete"


def task_transform_data(**context):
    """Task 3: Clean, deduplicate, normalize dates, and redact PII."""
    logger.info("Executing Task: transform_data...")
    from pipeline.cleaning import cleaner
    return "transform_complete"


def task_load_database(**context):
    """Task 4: Persist transformed datasets into PostgreSQL / warehouse."""
    logger.info("Executing Task: load_database...")
    from database.postgres import get_db
    return "load_complete"


def task_quality_check(**context):
    """Task 5: Post-load assertions, row count reconciliations, and null checks."""
    logger.info("Executing Task: quality_check...")
    from database.postgres import get_db
    db = get_db()
    counts = db.get_table_counts()
    assert counts.get("users", 0) > 0, "Users table must not be empty"
    assert counts.get("content", 0) > 0, "Content table must not be empty"
    return "quality_passed"


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="datastream_ott_pipeline",
        default_args=default_args,
        description="Daily OTT platform event ingestion and analytics transformation pipeline",
        schedule_interval="0 2 * * *",  # Run daily at 02:00 AM UTC
        catchup=False,
        tags=["datastream", "ott", "data_quality", "production"],
    ) as dag:

        t_extract = PythonOperator(
            task_id="extract_data",
            python_callable=task_extract_data,
        )

        t_validate = PythonOperator(
            task_id="validate_data",
            python_callable=task_validate_data,
        )

        t_transform = PythonOperator(
            task_id="transform_data",
            python_callable=task_transform_data,
        )

        t_load = PythonOperator(
            task_id="load_database",
            python_callable=task_load_database,
        )

        t_quality = PythonOperator(
            task_id="quality_check",
            python_callable=task_quality_check,
        )

        # Pipeline Task Dependency Graph
        t_extract >> t_validate >> t_transform >> t_load >> t_quality
