"""
DataStream AI - Data Quality & Validation Engine
Calculates real dynamic metrics:
- Total records, valid records, invalid records
- Duplicate records
- Missing values / null rates
- Invalid timestamps
- Invalid event types
- Out-of-bounds numeric values
- Overall dynamic Data Quality Score (0-100%)
"""

import re
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from pipeline.schema_drift import drift_detector

VALID_EVENT_TYPES = {
    "user_login",
    "content_view",
    "video_watch",
    "search",
    "add_to_watchlist",
    "subscription",
    "logout"
}

VALID_CONTENT_TYPES = {"Movie", "Series", "Documentary", "Short"}


class DataQualityEngine:
    def __init__(self):
        pass

    def validate_events(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Performs comprehensive data quality audit on events dataframe."""
        total_records = len(df)
        if total_records == 0:
            return self._empty_result("events")

        # 1. Duplicates
        duplicate_mask = df.duplicated(subset=["event_id"], keep=False)
        total_duplicates = int(duplicate_mask.sum())

        # 2. Missing values across columns
        missing_by_col = df.isnull().sum().to_dict()
        total_missing = int(df.isnull().sum().sum())

        # 3. Invalid IDs (e.g. null user_id or malformed ID pattern)
        invalid_user_id_mask = df["user_id"].isnull() | (df["user_id"].astype(str).str.strip() == "")
        invalid_user_ids = int(invalid_user_id_mask.sum())

        # 4. Invalid event types
        invalid_type_mask = ~df["event_type"].isin(VALID_EVENT_TYPES)
        invalid_event_types = int(invalid_type_mask.sum())

        # 5. Invalid numeric values (watch_time_mins < 0)
        numeric_series = pd.to_numeric(df["watch_time_mins"], errors="coerce")
        invalid_numeric_mask = numeric_series.isnull() | (numeric_series < 0)
        invalid_numerics = int(invalid_numeric_mask.sum())

        # 6. Invalid timestamps
        timestamp_series = pd.to_datetime(df["timestamp"], errors="coerce")
        invalid_time_mask = timestamp_series.isnull()
        invalid_timestamps = int(invalid_time_mask.sum())

        # Record-level validity (Row is invalid if ANY critical check fails)
        invalid_record_mask = (
            duplicate_mask |
            invalid_user_id_mask |
            invalid_type_mask |
            invalid_numeric_mask |
            invalid_time_mask
        )
        invalid_records = int(invalid_record_mask.sum())
        valid_records = total_records - invalid_records

        # Calculate dynamic Data Quality Score
        # Weights: valid_ratio (50%), uniqueness (25%), completeness (25%)
        valid_ratio = valid_records / total_records if total_records else 0
        uniqueness_ratio = 1.0 - (total_duplicates / total_records) if total_records else 1
        completeness_ratio = 1.0 - (total_missing / (total_records * len(df.columns))) if total_records else 1

        dq_score = round(
            (0.50 * valid_ratio + 0.25 * uniqueness_ratio + 0.25 * completeness_ratio) * 100, 1
        )
        dq_score = max(0.0, min(100.0, dq_score))

        # Schema Drift Check
        drift_report = drift_detector.check_drift("events", df)

        # Detailed error log samples
        error_samples = []
        for idx, row in df[invalid_record_mask].iterrows():
            reasons = []
            if duplicate_mask.iloc[idx]:
                reasons.append("Duplicate event_id")
            if invalid_user_id_mask.iloc[idx]:
                reasons.append("Missing user_id")
            if invalid_type_mask.iloc[idx]:
                reasons.append(f"Invalid event_type '{row['event_type']}'")
            if invalid_numeric_mask.iloc[idx]:
                reasons.append(f"Negative/malformed watch_time ({row['watch_time_mins']})")
            if invalid_time_mask.iloc[idx]:
                reasons.append(f"Malformed timestamp '{row['timestamp']}'")

            error_samples.append({
                "row_index": int(idx),
                "event_id": str(row.get("event_id", "N/A")),
                "reasons": ", ".join(reasons)
            })

        return {
            "dataset": "events",
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "duplicates": total_duplicates,
            "missing_values": total_missing,
            "missing_by_column": missing_by_col,
            "invalid_user_ids": invalid_user_ids,
            "invalid_event_types": invalid_event_types,
            "invalid_numerics": invalid_numerics,
            "invalid_timestamps": invalid_timestamps,
            "dq_score": dq_score,
            "schema_warnings": drift_report["warnings"],
            "has_drift": drift_report["has_drift"],
            "error_samples": error_samples,
            "invalid_mask": invalid_record_mask
        }

    def validate_users(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Data quality audit on users dataframe."""
        total_records = len(df)
        if total_records == 0:
            return self._empty_result("users")

        duplicate_mask = df.duplicated(subset=["user_id"], keep=False)
        total_duplicates = int(duplicate_mask.sum())

        missing_by_col = df.isnull().sum().to_dict()
        total_missing = int(df.isnull().sum().sum())

        # Check required fields: user_id, email, registration_date
        missing_id_mask = df["user_id"].isnull() | (df["user_id"].astype(str).str.strip() == "")
        missing_email_mask = df["email"].isnull() | (~df["email"].astype(str).str.contains("@", na=False))
        invalid_reg_date_mask = pd.to_datetime(df["registration_date"], errors="coerce").isnull()

        invalid_record_mask = duplicate_mask | missing_id_mask | missing_email_mask | invalid_reg_date_mask
        invalid_records = int(invalid_record_mask.sum())
        valid_records = total_records - invalid_records

        valid_ratio = valid_records / total_records if total_records else 0
        uniqueness_ratio = 1.0 - (total_duplicates / total_records) if total_records else 1
        completeness_ratio = 1.0 - (total_missing / (total_records * len(df.columns))) if total_records else 1

        dq_score = round(
            (0.50 * valid_ratio + 0.25 * uniqueness_ratio + 0.25 * completeness_ratio) * 100, 1
        )

        drift_report = drift_detector.check_drift("users", df)

        error_samples = []
        for idx, row in df[invalid_record_mask].iterrows():
            reasons = []
            if duplicate_mask.iloc[idx]:
                reasons.append("Duplicate user_id")
            if missing_id_mask.iloc[idx]:
                reasons.append("Missing user_id")
            if missing_email_mask.iloc[idx]:
                reasons.append("Malformed or missing email")
            if invalid_reg_date_mask.iloc[idx]:
                reasons.append("Invalid registration date")
            error_samples.append({
                "row_index": int(idx),
                "user_id": str(row.get("user_id", "N/A")),
                "reasons": ", ".join(reasons)
            })

        return {
            "dataset": "users",
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "duplicates": total_duplicates,
            "missing_values": total_missing,
            "missing_by_column": missing_by_col,
            "dq_score": dq_score,
            "schema_warnings": drift_report["warnings"],
            "has_drift": drift_report["has_drift"],
            "error_samples": error_samples,
            "invalid_mask": invalid_record_mask
        }

    def _empty_result(self, dataset_name: str) -> Dict[str, Any]:
        return {
            "dataset": dataset_name,
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "duplicates": 0,
            "missing_values": 0,
            "missing_by_column": {},
            "dq_score": 100.0,
            "schema_warnings": [],
            "has_drift": False,
            "error_samples": [],
            "invalid_mask": pd.Series(dtype=bool)
        }


dq_engine = DataQualityEngine()
