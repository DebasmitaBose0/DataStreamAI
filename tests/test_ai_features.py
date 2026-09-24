"""
Unit and Integration Tests for DataStream AI:
- Subscriber Churn Predictor
- Personalized Content Recommender
- Streaming Anomaly & Fraud Radar
"""

import pytest
import pandas as pd
from ai.churn_predictor import churn_engine
from ai.recommendation import content_recommender
from pipeline.anomaly_detector import anomaly_radar


class TestChurnPredictor:
    def test_predict_all_users_structure(self):
        predictions = churn_engine.predict_all_users()
        assert not predictions.empty
        assert "user_id" in predictions.columns
        assert "churn_risk_score" in predictions.columns
        assert "risk_level" in predictions.columns
        assert "retention_action" in predictions.columns

    def test_predict_single_user(self):
        pred = churn_engine.predict_user("U1001")
        assert pred is not None
        assert "churn_risk_score" in pred
        assert 0 <= pred["churn_risk_score"] <= 100
        assert pred["risk_level"] in ["Low", "Medium", "High"]

    def test_fleet_summary(self):
        summary = churn_engine.get_fleet_summary()
        assert summary["total_users"] > 0
        assert "avg_risk" in summary
        assert "high_risk" in summary


class TestContentRecommender:
    def test_recommend_for_active_user(self):
        recs = content_recommender.recommend_for_user("U1001", top_n=3)
        assert isinstance(recs, list)
        assert len(recs) <= 3
        if recs:
            first = recs[0]
            assert "title" in first
            assert "genre" in first
            assert "match_score" in first
            assert "reason" in first

    def test_cold_start_user(self):
        # Non-existent user should fallback to top rated
        recs = content_recommender.recommend_for_user("U9999", top_n=3)
        assert len(recs) > 0
        assert recs[0]["rating"] >= 7.0

    def test_similar_content(self):
        sims = content_recommender.get_similar_content("MOV101", top_n=3)
        assert isinstance(sims, list)
        if sims:
            assert "similarity_score" in sims[0]


class TestAnomalyDetector:
    def test_anomaly_analysis_returns_list(self):
        anomalies = anomaly_radar.analyze_anomalies()
        assert isinstance(anomalies, list)
        assert len(anomalies) > 0
        assert "anomaly_id" in anomalies[0]
        assert "severity" in anomalies[0]

    def test_summary_metrics(self):
        summary = anomaly_radar.get_summary_metrics()
        assert "fleet_status" in summary
        assert "critical_count" in summary
