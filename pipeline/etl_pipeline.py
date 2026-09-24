"""
DataStream AI - End-to-End ETL Pipeline & Resilience Engine
Extracts CSV/streaming data, Transforms (cleans, validates, deduplicates, masks PII),
Loads into Relational Database (PostgreSQL/SQLite) and Document Store (MongoDB/JSON).
Includes basic self-healing/resilience retry prototype.
"""

import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
from sqlalchemy import text

from database.postgres import get_db
from database.mongodb import get_mongo
from pipeline.ingestion import ingestion_engine
from pipeline.cleaning import cleaner
from pipeline.validation import dq_engine
from pipeline.schema_drift import drift_detector

logger = logging.getLogger("datastream.etl")
BASE_DIR = Path(__file__).resolve().parent.parent


class ETLPipeline:
    def __init__(self):
        self.db = get_db()
        self.mongo = get_mongo()
        self.execution_logs: List[Dict[str, Any]] = []

    def run_pipeline(
        self,
        max_retries: int = 3,
        simulate_failure_once: bool = False,
        mask_pii: bool = False,
        force_recreate_schema: bool = False
    ) -> Dict[str, Any]:
        """
        Executes the full ETL cycle with self-healing retry logic.
        """
        start_time = datetime.now()
        run_id = f"etl_run_{start_time.strftime('%Y%m%d_%H%M%S')}"
        pipeline_status = "RUNNING"
        retries_attempted = 0
        error_message = None

        log_events = []
        def log_step(msg: str, level: str = "INFO"):
            log_events.append({"time": datetime.now().strftime("%H:%M:%S.%f")[:-3], "level": level, "message": msg})
            logger.info("[%s] %s", run_id, msg)

        log_step(f"Initiating DataStream AI ETL pipeline run {run_id}")
        log_step(f"Database Target: {self.db.db_type} | Document Target: {self.mongo.storage_mode}")

        # Ensure schema exists
        self.db.init_schema(force_reset=force_recreate_schema)

        # ----------------------------------------------------
        # Phase 1: EXTRACT (With Retry Logic)
        # ----------------------------------------------------
        extracted_data = {}
        for attempt in range(1, max_retries + 1):
            try:
                log_step(f"Extract Phase: Attempt {attempt}/{max_retries}...")
                
                # Prototype resilience test: simulate transient glitch if requested
                if simulate_failure_once and attempt == 1:
                    retries_attempted += 1
                    raise ConnectionResetError("Simulated transient network timeout accessing primary raw data stream.")

                data_dir = BASE_DIR / "data"
                extracted_data["users"] = pd.read_csv(data_dir / "users.csv")
                extracted_data["content"] = pd.read_csv(data_dir / "content.csv")
                extracted_data["subscriptions"] = pd.read_csv(data_dir / "subscriptions.csv")
                extracted_data["events"] = pd.read_csv(data_dir / "events.csv")
                log_step(f"Extraction successful: Extracted {sum(len(df) for df in extracted_data.values())} raw records across 4 datasets.")
                break
            except Exception as e:
                retries_attempted += 1
                log_step(f"Self-Healing Triggered: Extraction attempt {attempt} failed ({str(e)}). Retrying in 0.5s...", "WARNING")
                time.sleep(0.5)
                if attempt == max_retries:
                    pipeline_status = "FAILED"
                    error_message = f"Extraction failed after {max_retries} attempts: {e}"
                    log_step(error_message, "ERROR")
                    return self._finalize_run(run_id, start_time, pipeline_status, retries_attempted, error_message, log_events, {})

        # ----------------------------------------------------
        # Phase 2: AUDIT & DATA QUALITY ASSESSMENT
        # ----------------------------------------------------
        log_step("Data Quality Phase: Auditing raw datasets before transformation...")
        events_audit = dq_engine.validate_events(extracted_data["events"])
        users_audit = dq_engine.validate_users(extracted_data["users"])
        content_drift = drift_detector.check_drift("content", extracted_data["content"])

        log_step(f"Data Quality Audit: Events DQ Score: {events_audit['dq_score']}% | Users DQ Score: {users_audit['dq_score']}%")
        if events_audit["schema_warnings"]:
            for w in events_audit["schema_warnings"]:
                log_step(f"Schema Warning: {w}", "WARNING")

        # ----------------------------------------------------
        # Phase 3: TRANSFORM & CLEAN
        # ----------------------------------------------------
        log_step("Transform Phase: Cleaning, deduplicating, standardizing, and masking PII...")
        clean_users_df, u_stats = cleaner.clean_users(extracted_data["users"], mask_pii=mask_pii)
        clean_content_df, c_stats = cleaner.clean_content(extracted_data["content"])
        clean_subs_df, s_stats = cleaner.clean_subscriptions(extracted_data["subscriptions"])
        clean_events_df, e_stats = cleaner.clean_events(extracted_data["events"], drop_invalid=True)

        log_step(f"Transform Stats: Users ({u_stats['cleaned_rows']}/{u_stats['initial_rows']}), Events ({e_stats['cleaned_rows']}/{e_stats['initial_rows']} kept, {e_stats['invalid_dropped']} invalid dropped)")

        # Derive watch_history summary mart from cleaned events
        watch_history_df = self._derive_watch_history(clean_events_df, clean_content_df)
        log_step(f"Derived {len(watch_history_df)} watch_history engagement records from events.")

        # ----------------------------------------------------
        # Phase 4: LOAD INTO DATABASES
        # ----------------------------------------------------
        log_step("Load Phase: Persisting cleaned data into relational tables...")
        try:
            with self.db.engine.begin() as conn:
                # Clear and load in relational dependency order
                conn.execute(text("DELETE FROM watch_history;"))
                conn.execute(text("DELETE FROM events;"))
                conn.execute(text("DELETE FROM subscriptions;"))
                conn.execute(text("DELETE FROM content;"))
                conn.execute(text("DELETE FROM users;"))

                clean_users_df.to_sql("users", conn, if_exists="append", index=False)
                clean_content_df.to_sql("content", conn, if_exists="append", index=False)
                clean_subs_df.to_sql("subscriptions", conn, if_exists="append", index=False)
                clean_events_df.to_sql("events", conn, if_exists="append", index=False)
                if not watch_history_df.empty:
                    watch_history_df.to_sql("watch_history", conn, if_exists="append", index=False)

            log_step("Relational database load complete.")
        except Exception as e:
            log_step(f"Relational load encountered error: {e}", "ERROR")
            pipeline_status = "FAILED"
            return self._finalize_run(run_id, start_time, pipeline_status, retries_attempted, str(e), log_events, {})

        # Load raw events to MongoDB / Document Store
        log_step("Load Phase: Storing raw un-redacted events in document store...")
        try:
            raw_docs = []
            for _, row in extracted_data["events"].iterrows():
                doc = {
                    "event_id": str(row.get("event_id", "")),
                    "user_id": str(row.get("user_id", "")),
                    "event": str(row.get("event_type", "")),
                    "content_id": str(row.get("content_id", "")),
                    "timestamp": str(row.get("timestamp", "")),
                    "metadata": {
                        "device": str(row.get("device", "")),
                        "watch_time_mins": int(pd.to_numeric(row.get("watch_time_mins", 0), errors="coerce") or 0),
                        "session_id": str(row.get("session_id", "")),
                        "ip_address": str(row.get("ip_address", ""))
                    }
                }
                raw_docs.append(doc)
            inserted_docs = self.mongo.insert_many_events(raw_docs)
            log_step(f"Document store load complete: {inserted_docs} raw events stored.")
        except Exception as e:
            log_step(f"Document store non-critical notice: {e}", "WARNING")

        pipeline_status = "SUCCESS"
        log_step(f"Pipeline {run_id} completed successfully.")

        metrics = {
            "total_extracted": sum(len(df) for df in extracted_data.values()),
            "users_loaded": len(clean_users_df),
            "content_loaded": len(clean_content_df),
            "subs_loaded": len(clean_subs_df),
            "events_loaded": len(clean_events_df),
            "watch_history_derived": len(watch_history_df),
            "events_dq_score": events_audit["dq_score"],
            "users_dq_score": users_audit["dq_score"],
            "invalid_dropped": e_stats["invalid_dropped"],
            "duplicates_removed": e_stats["duplicates_removed"] + u_stats["duplicates_removed"]
        }

        return self._finalize_run(run_id, start_time, pipeline_status, retries_attempted, None, log_events, metrics)

    def _derive_watch_history(self, events_df: pd.DataFrame, content_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregates video watch sessions into watch_history records."""
        watch_events = events_df[events_df["event_type"] == "video_watch"].copy()
        if watch_events.empty:
            return pd.DataFrame()

        # Merge with content duration to calculate completion %
        merged = watch_events.merge(content_df[["content_id", "duration_mins"]], on="content_id", how="left")
        merged["duration_mins"] = merged["duration_mins"].fillna(90)

        grouped = merged.groupby(["user_id", "content_id"]).agg(
            total_watch_time_mins=("watch_time_mins", "sum"),
            last_watched_at=("timestamp", "max"),
            duration_mins=("duration_mins", "first")
        ).reset_index()

        grouped["history_id"] = [f"WH_{i+1:04d}" for i in range(len(grouped))]
        grouped["completion_percentage"] = (grouped["total_watch_time_mins"] / grouped["duration_mins"] * 100).round(1)
        grouped["completion_percentage"] = grouped["completion_percentage"].clip(upper=100.0)
        grouped["completed"] = grouped["completion_percentage"] >= 90.0

        return grouped[["history_id", "user_id", "content_id", "total_watch_time_mins", "completion_percentage", "last_watched_at", "completed"]]

    def _finalize_run(self, run_id, start_time, status, retries, error, log_events, metrics) -> Dict[str, Any]:
        duration = round((datetime.now() - start_time).total_seconds(), 2)
        summary = {
            "run_id": run_id,
            "timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "duration_seconds": duration,
            "retries_attempted": retries,
            "error_message": error,
            "logs": log_events,
            "metrics": metrics,
            "db_type": self.db.db_type,
            "mongo_storage": self.mongo.storage_mode
        }
        self.execution_logs.append(summary)
        return summary

    def get_latest_run(self) -> Optional[Dict[str, Any]]:
        return self.execution_logs[-1] if self.execution_logs else None

    def get_history(self) -> List[Dict[str, Any]]:
        return self.execution_logs


etl_runner = ETLPipeline()
