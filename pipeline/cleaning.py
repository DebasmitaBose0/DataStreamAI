"""
DataStream AI - Data Cleaning & Normalization Engine
Performs data transformations, deduplication, timestamp normalization,
type conversions, and privacy redaction.
"""

from typing import Tuple, Dict, Any
import pandas as pd
import numpy as np
from pipeline.privacy import mask_dataframe
from pipeline.validation import VALID_EVENT_TYPES


class DataCleaner:
    def __init__(self):
        pass

    def clean_users(self, df: pd.DataFrame, mask_pii: bool = False) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Cleans and standardizes the users dataset."""
        initial_count = len(df)
        df_clean = df.copy()

        # 1. Deduplicate
        df_clean = df_clean.drop_duplicates(subset=["user_id"], keep="first")
        duplicates_removed = initial_count - len(df_clean)

        # 2. Handle missing / string cleaning
        df_clean["name"] = df_clean["name"].fillna("Anonymous User").astype(str).str.strip()
        df_clean["country"] = df_clean["country"].fillna("Global").astype(str).str.strip()
        df_clean["tier"] = df_clean["tier"].fillna("Basic").astype(str).str.strip()
        df_clean["status"] = df_clean["status"].fillna("active").astype(str).str.strip()

        # 3. Standardize dates
        df_clean["registration_date"] = pd.to_datetime(df_clean["registration_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df_clean = df_clean.dropna(subset=["registration_date", "user_id"])

        # 4. Optional PII masking
        if mask_pii:
            df_clean = mask_dataframe(df_clean)

        stats = {
            "initial_rows": initial_count,
            "cleaned_rows": len(df_clean),
            "duplicates_removed": duplicates_removed,
            "pii_masked": mask_pii
        }
        return df_clean, stats

    def clean_content(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Cleans and standardizes OTT content catalog."""
        initial_count = len(df)
        df_clean = df.copy()

        # Deduplicate
        df_clean = df_clean.drop_duplicates(subset=["content_id"], keep="first")
        duplicates_removed = initial_count - len(df_clean)

        # Formats and fills
        df_clean["title"] = df_clean["title"].fillna("Untitled Content").astype(str).str.strip()
        df_clean["content_type"] = df_clean["content_type"].fillna("Movie").astype(str).str.strip()
        df_clean["genre"] = df_clean["genre"].fillna("General").astype(str).str.strip()
        df_clean["release_year"] = pd.to_numeric(df_clean["release_year"], errors="coerce").fillna(2023).astype(int)
        df_clean["duration_mins"] = pd.to_numeric(df_clean["duration_mins"], errors="coerce").fillna(90).astype(int)
        df_clean["rating"] = pd.to_numeric(df_clean["rating"], errors="coerce").fillna(7.0).round(1)
        df_clean["director"] = df_clean["director"].fillna("Unknown Director").astype(str).str.strip()

        stats = {
            "initial_rows": initial_count,
            "cleaned_rows": len(df_clean),
            "duplicates_removed": duplicates_removed
        }
        return df_clean, stats

    def clean_subscriptions(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Cleans and standardizes subscriptions dataset."""
        initial_count = len(df)
        df_clean = df.copy()

        # Deduplicate
        df_clean = df_clean.drop_duplicates(subset=["sub_id"], keep="first")
        duplicates_removed = initial_count - len(df_clean)

        df_clean["plan_name"] = df_clean["plan_name"].fillna("Standard HD").astype(str).str.strip()
        df_clean["monthly_price"] = pd.to_numeric(df_clean["monthly_price"], errors="coerce").fillna(9.99).abs().round(2)
        df_clean["status"] = df_clean["status"].fillna("active").astype(str).str.strip()
        df_clean["payment_method"] = df_clean["payment_method"].fillna("credit_card").astype(str).str.strip()

        df_clean["start_date"] = pd.to_datetime(df_clean["start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df_clean["renewal_date"] = pd.to_datetime(df_clean["renewal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df_clean = df_clean.dropna(subset=["sub_id", "user_id", "start_date"])

        stats = {
            "initial_rows": initial_count,
            "cleaned_rows": len(df_clean),
            "duplicates_removed": duplicates_removed
        }
        return df_clean, stats

    def clean_events(self, df: pd.DataFrame, drop_invalid: bool = True, mask_ips: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Cleans and transforms streaming events dataset."""
        initial_count = len(df)
        df_clean = df.copy()

        # Deduplicate
        df_clean = df_clean.drop_duplicates(subset=["event_id"], keep="first")
        duplicates_removed = initial_count - len(df_clean)

        # Standardize timestamps
        df_clean["timestamp_parsed"] = pd.to_datetime(df_clean["timestamp"], errors="coerce")

        # Sanitize watch_time_mins (set negative to 0)
        df_clean["watch_time_mins"] = pd.to_numeric(df_clean["watch_time_mins"], errors="coerce").fillna(0)
        df_clean["watch_time_mins"] = df_clean["watch_time_mins"].apply(lambda x: max(0, int(x)))

        # Ensure optional columns are present
        if "device" not in df_clean.columns:
            df_clean["device"] = "Smart TV"
        else:
            df_clean["device"] = df_clean["device"].fillna("Smart TV").astype(str).str.strip()

        if "session_id" not in df_clean.columns:
            df_clean["session_id"] = "sess_gen"
        else:
            df_clean["session_id"] = df_clean["session_id"].fillna("sess_gen").astype(str).str.strip()

        if drop_invalid:
            # Filter valid event types
            valid_type_mask = df_clean["event_type"].isin(VALID_EVENT_TYPES)
            # Filter non-null user_id and valid timestamp
            valid_user_mask = df_clean["user_id"].notnull() & (df_clean["user_id"].astype(str).str.strip() != "")
            valid_time_mask = df_clean["timestamp_parsed"].notnull()

            valid_mask = valid_type_mask & valid_user_mask & valid_time_mask
            invalid_dropped = int((~valid_mask).sum())
            df_clean = df_clean[valid_mask].copy()
        else:
            invalid_dropped = 0

        df_clean["timestamp"] = df_clean["timestamp_parsed"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_clean = df_clean.drop(columns=["timestamp_parsed"])

        if mask_ips and "ip_address" in df_clean.columns:
            df_clean = mask_dataframe(df_clean, mask_emails=False, mask_phones=False, mask_ips=True)

        stats = {
            "initial_rows": initial_count,
            "cleaned_rows": len(df_clean),
            "duplicates_removed": duplicates_removed,
            "invalid_dropped": invalid_dropped
        }
        return df_clean, stats


cleaner = DataCleaner()
