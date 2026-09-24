"""
DataStream AI - Automated Test Suite
Covers:
1. Data Validation & Rules
2. Duplicate Detection
3. Schema Drift Detection
4. PII Redaction & Masking
5. ETL Transformation
6. SQL Analytics Execution
"""

import pytest
import pandas as pd
from pipeline.validation import dq_engine
from pipeline.schema_drift import drift_detector
from pipeline.privacy import mask_email, mask_phone, mask_ip, mask_dataframe
from pipeline.cleaning import cleaner
from database.postgres import get_db


class TestPrivacyAndPII:
    def test_mask_email(self):
        email = "debasmita.roy@gmail.com"
        masked = mask_email(email)
        assert masked != email
        assert masked.startswith("d")
        assert masked.endswith("@gmail.com")
        assert "*" in masked

    def test_mask_phone(self):
        phone = "+91-98765-43210"
        masked = mask_phone(phone)
        assert masked.endswith("3210")
        assert "*" in masked

    def test_mask_ip(self):
        ip = "192.168.1.45"
        masked = mask_ip(ip)
        assert masked == "192.168.*.*"

    def test_mask_dataframe(self):
        df = pd.DataFrame({
            "user_id": ["U101"],
            "email": ["test.user@example.com"],
            "phone": ["+1-555-123-4567"],
            "ip_address": ["10.0.0.1"]
        })
        masked_df = mask_dataframe(df)
        assert "*" in masked_df["email"].iloc[0]
        assert "*" in masked_df["phone"].iloc[0]
        assert masked_df["ip_address"].iloc[0] == "10.0.*.*"


class TestDataQualityAndValidation:
    def test_duplicate_detection(self):
        df_duplicates = pd.DataFrame({
            "event_id": ["EVT1", "EVT1", "EVT2"],
            "user_id": ["U101", "U101", "U102"],
            "event_type": ["video_watch", "video_watch", "video_watch"],
            "watch_time_mins": [10, 10, 20],
            "timestamp": ["2024-03-01 10:00:00", "2024-03-01 10:00:00", "2024-03-01 11:00:00"]
        })
        res = dq_engine.validate_events(df_duplicates)
        assert res["duplicates"] == 2
        assert res["valid_records"] < len(df_duplicates)

    def test_invalid_event_type_rejection(self):
        df_invalid = pd.DataFrame({
            "event_id": ["EVT99"],
            "user_id": ["U101"],
            "event_type": ["hacked_unknown_action"],
            "watch_time_mins": [10],
            "timestamp": ["2024-03-01 10:00:00"]
        })
        res = dq_engine.validate_events(df_invalid)
        assert res["invalid_event_types"] == 1
        assert res["valid_records"] == 0

    def test_negative_numeric_rejection(self):
        df_negative = pd.DataFrame({
            "event_id": ["EVT88"],
            "user_id": ["U101"],
            "event_type": ["video_watch"],
            "watch_time_mins": [-50],
            "timestamp": ["2024-03-01 10:00:00"]
        })
        res = dq_engine.validate_events(df_negative)
        assert res["invalid_numerics"] == 1


class TestSchemaDrift:
    def test_new_column_detection(self):
        df = pd.DataFrame({
            "user_id": ["U1"], "name": ["A"], "email": ["a@b.com"],
            "phone": ["123"], "country": ["US"], "registration_date": ["2024-01-01"],
            "tier": ["Basic"], "status": ["active"],
            "unexpected_new_column": [True]  # Drift!
        })
        report = drift_detector.check_drift("users", df)
        assert report["has_drift"] is True
        assert "unexpected_new_column" in report["added_columns"]

    def test_missing_column_detection(self):
        df = pd.DataFrame({"user_id": ["U1"]})  # missing name, email, etc.
        report = drift_detector.check_drift("users", df)
        assert report["has_drift"] is True
        assert "email" in report["missing_columns"]


class TestCleaningAndTransformation:
    def test_event_cleaning_deduplication(self):
        df = pd.DataFrame({
            "event_id": ["E1", "E1", "E2"],
            "user_id": ["U1", "U1", "U2"],
            "event_type": ["video_watch", "video_watch", "search"],
            "watch_time_mins": [30, 30, 0],
            "timestamp": ["2024-03-01 10:00:00", "2024-03-01 10:00:00", "2024-03-01 10:05:00"]
        })
        cleaned, stats = cleaner.clean_events(df)
        assert len(cleaned) == 2
        assert stats["duplicates_removed"] == 1


class TestDatabaseAndSQL:
    def test_database_query_execution(self):
        db = get_db()
        df = db.query("SELECT 1 AS test_col;")
        assert not df.empty
        assert df.iloc[0]["test_col"] == 1
