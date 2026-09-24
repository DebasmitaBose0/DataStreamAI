"""
DataStream AI - Schema Drift Detection Engine
Compares incoming dataset schema against expected baseline schemas.
Detects:
- Added columns (New fields)
- Missing columns (Dropped/Renamed fields)
- Data type discrepancies
"""

from typing import Dict, List, Any
import pandas as pd

# Baseline Expected Schemas
BASELINE_SCHEMAS: Dict[str, Dict[str, str]] = {
    "users": {
        "user_id": "object",
        "name": "object",
        "email": "object",
        "phone": "object",
        "country": "object",
        "registration_date": "object",
        "tier": "object",
        "status": "object"
    },
    "content": {
        "content_id": "object",
        "title": "object",
        "content_type": "object",
        "genre": "object",
        "release_year": "int64",
        "duration_mins": "int64",
        "rating": "float64",
        "director": "object"
    },
    "subscriptions": {
        "sub_id": "object",
        "user_id": "object",
        "plan_name": "object",
        "monthly_price": "float64",
        "start_date": "object",
        "renewal_date": "object",
        "status": "object",
        "payment_method": "object"
    },
    "events": {
        "event_id": "object",
        "user_id": "object",
        "event_type": "object",
        "content_id": "object",
        "watch_time_mins": "int64",
        "device": "object",
        "session_id": "object",
        "timestamp": "object",
        "ip_address": "object"
    }
}


class SchemaDriftDetector:
    def __init__(self, baselines: Dict[str, Dict[str, str]] = None):
        self.baselines = baselines or BASELINE_SCHEMAS

    def check_drift(self, dataset_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyzes the DataFrame against its baseline schema.
        Returns drift status, new columns, missing columns, type mismatches, and warnings.
        """
        if dataset_name not in self.baselines:
            return {
                "dataset": dataset_name,
                "has_drift": False,
                "warnings": [f"No baseline schema registered for '{dataset_name}'."],
                "added_columns": [],
                "missing_columns": [],
                "type_mismatches": []
            }

        expected_schema = self.baselines[dataset_name]
        expected_cols = set(expected_schema.keys())
        current_cols = set(df.columns)

        added_cols = list(current_cols - expected_cols)
        missing_cols = list(expected_cols - current_cols)

        type_mismatches = []
        for col in expected_cols.intersection(current_cols):
            expected_type = expected_schema[col]
            current_type = str(df[col].dtype)
            # Basic fuzzy compatibility check
            if not self._types_compatible(expected_type, current_type):
                type_mismatches.append({
                    "column": col,
                    "expected": expected_type,
                    "actual": current_type
                })

        warnings = []
        if added_cols:
            for col in added_cols:
                warnings.append(f"WARNING: New column detected: '{col}' (Potential Schema Expansion)")
        if missing_cols:
            for col in missing_cols:
                warnings.append(f"CRITICAL: Missing expected column: '{col}'")
        if type_mismatches:
            for tm in type_mismatches:
                warnings.append(f"WARNING: Column '{tm['column']}' expected {tm['expected']}, got {tm['actual']}")

        has_drift = bool(added_cols or missing_cols or type_mismatches)

        return {
            "dataset": dataset_name,
            "has_drift": has_drift,
            "drift_severity": "High" if missing_cols else ("Medium" if type_mismatches else ("Low" if added_cols else "None")),
            "added_columns": added_cols,
            "missing_columns": missing_cols,
            "type_mismatches": type_mismatches,
            "warnings": warnings,
            "current_columns": list(df.columns),
            "expected_columns": list(expected_schema.keys())
        }

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Helper to check if types are broadly compatible (e.g. int32 vs int64, float32 vs float64)"""
        if expected == actual:
            return True
        if "int" in expected and "int" in actual:
            return True
        if "float" in expected and ("float" in actual or "int" in actual):
            return True
        if expected == "object" and ("str" in actual or "object" in actual):
            return True
        return False


drift_detector = SchemaDriftDetector()
