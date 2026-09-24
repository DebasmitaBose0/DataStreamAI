"""
DataStream AI - Data Ingestion Engine
Handles loading of CSV data files, runs pre-ingestion audits,
tracks metrics, logs ingestion status, and exposes summary statistics.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
from pipeline.validation import dq_engine
from pipeline.schema_drift import drift_detector

logger = logging.getLogger("datastream.ingestion")
BASE_DIR = Path(__file__).resolve().parent.parent


class IngestionEngine:
    def __init__(self):
        self.ingestion_history: List[Dict[str, Any]] = []

    def load_dataset(self, file_path: str, dataset_name: str) -> Dict[str, Any]:
        """
        Loads CSV dataset from disk or buffer, audits schema and quality,
        and logs the ingestion run.
        """
        start_time = datetime.now()
        path_obj = Path(file_path)

        if not path_obj.exists():
            return {
                "success": False,
                "dataset": dataset_name,
                "error": f"File not found: {file_path}",
                "records_loaded": 0
            }

        try:
            df = pd.read_csv(file_path)
            total_records = len(df)

            # Perform validation based on dataset type
            if dataset_name == "events":
                dq_result = dq_engine.validate_events(df)
            elif dataset_name == "users":
                dq_result = dq_engine.validate_users(df)
            else:
                drift_rep = drift_detector.check_drift(dataset_name, df)
                dq_result = {
                    "dataset": dataset_name,
                    "total_records": total_records,
                    "valid_records": total_records,
                    "invalid_records": 0,
                    "duplicates": int(df.duplicated().sum()),
                    "missing_values": int(df.isnull().sum().sum()),
                    "dq_score": 100.0,
                    "schema_warnings": drift_rep["warnings"],
                    "has_drift": drift_rep["has_drift"],
                    "error_samples": []
                }

            execution_duration = (datetime.now() - start_time).total_seconds()

            summary = {
                "run_id": f"ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{dataset_name}",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "dataset_name": dataset_name,
                "file_path": str(file_path),
                "total_records": total_records,
                "valid_records": dq_result["valid_records"],
                "invalid_records": dq_result["invalid_records"],
                "duplicates": dq_result["duplicates"],
                "missing_values": dq_result["missing_values"],
                "dq_score": dq_result["dq_score"],
                "schema_warnings_count": len(dq_result["schema_warnings"]),
                "schema_warnings": dq_result["schema_warnings"],
                "duration_seconds": round(execution_duration, 3),
                "status": "COMPLETED_WITH_WARNINGS" if dq_result["schema_warnings"] or dq_result["invalid_records"] > 0 else "SUCCESS",
                "df": df,
                "dq_details": dq_result
            }

            self.ingestion_history.append(summary)
            logger.info("Ingestion completed for %s: %d records, status=%s", dataset_name, total_records, summary["status"])
            return {"success": True, **summary}

        except Exception as e:
            logger.error("Failed to ingest %s: %s", dataset_name, e)
            return {
                "success": False,
                "dataset": dataset_name,
                "error": str(e),
                "records_loaded": 0
            }

    def load_all_sample_datasets(self) -> Dict[str, Any]:
        """Loads all standard CSV files from data/ directory."""
        data_dir = BASE_DIR / "data"
        results = {}
        datasets = [
            ("users.csv", "users"),
            ("content.csv", "content"),
            ("subscriptions.csv", "subscriptions"),
            ("events.csv", "events")
        ]
        for filename, ds_name in datasets:
            file_p = data_dir / filename
            if file_p.exists():
                results[ds_name] = self.load_dataset(str(file_p), ds_name)
        return results

    def get_history(self) -> List[Dict[str, Any]]:
        return self.ingestion_history


ingestion_engine = IngestionEngine()
