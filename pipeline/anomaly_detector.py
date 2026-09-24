"""
DataStream AI - Streaming Anomaly & Security Radar
Detects real-time QoS anomalies, buffering spikes, concurrent account sharing,
and bot/crawler traffic behaviors on OTT streaming events.
"""

from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
import numpy as np
from database.postgres import get_db


class StreamingAnomalyDetector:
    def __init__(self):
        self.db = get_db()

    def analyze_anomalies(self, events_df: pd.DataFrame = None) -> List[Dict[str, Any]]:
        """Scans the event stream for operational and security anomalies."""
        if events_df is None or events_df.empty:
            events_df = self.db.query("SELECT * FROM events;")

        if events_df.empty:
            return []

        anomalies = []

        # 1. Account Sharing / Concurrent IP Anomaly
        user_ips = events_df.groupby("user_id")["ip_address"].nunique()
        multi_ip_users = user_ips[user_ips > 2]
        for uid, ip_count in multi_ip_users.items():
            user_devs = events_df[events_df["user_id"] == uid]["device"].unique().tolist()
            anomalies.append({
                "anomaly_id": f"ANO-ACC-{uid}",
                "category": "Security / Credential Sharing",
                "severity": "CRITICAL" if ip_count >= 3 else "WARNING",
                "affected_entity": f"User {uid}",
                "description": f"Streaming activity detected across {ip_count} distinct IP addresses and multiple devices ({', '.join(user_devs[:3])}).",
                "metric_value": f"{ip_count} IPs",
                "recommended_action": "Trigger 2FA re-verification or prompt for Family Plan upgrade."
            })

        # 2. Buffering & QoS Degradation Spike
        event_types = events_df["event_type"].value_counts(normalize=True)
        buffer_ratio = event_types.get("buffering", 0.0) + event_types.get("error", 0.0)
        if buffer_ratio > 0.10:
            anomalies.append({
                "anomaly_id": "ANO-QOS-01",
                "category": "QoS / Network Degradation",
                "severity": "HIGH",
                "affected_entity": "CDN Streaming Cluster",
                "description": f"Buffering & playback friction events represent {round(buffer_ratio * 100, 1)}% of total stream traffic (threshold: 10%).",
                "metric_value": f"{round(buffer_ratio * 100, 1)}% friction",
                "recommended_action": "Auto-switch CDN edge POP or downscale initial adaptive bitrate (ABR) profile."
            })

        # 3. Excessive Session Duration (Bot / Screen-Recorder Anomaly)
        watch_events = events_df[events_df["event_type"] == "video_watch"]
        if not watch_events.empty and "watch_time_mins" in watch_events.columns:
            extreme_sessions = watch_events[watch_events["watch_time_mins"] > 240]
            for _, sess in extreme_sessions.iterrows():
                anomalies.append({
                    "anomaly_id": f"ANO-BOT-{sess['event_id']}",
                    "category": "Abnormal Playback Duration",
                    "severity": "MEDIUM",
                    "affected_entity": f"User {sess['user_id']} ({sess['device']})",
                    "description": f"Continuous playback session exceeded {sess['watch_time_mins']} minutes without user interaction.",
                    "metric_value": f"{sess['watch_time_mins']} mins",
                    "recommended_action": "Send 'Are you still watching?' prompt to preserve CDN bandwidth."
                })

        # 4. Rapid Search / Zero Playback Scraping (Bot Detection)
        user_grouped = events_df.groupby("user_id")
        for uid, grp in user_grouped:
            searches = (grp["event_type"] == "search").sum()
            watches = (grp["event_type"] == "video_watch").sum()
            if searches >= 5 and watches == 0:
                anomalies.append({
                    "anomaly_id": f"ANO-SCRAP-{uid}",
                    "category": "Automated Crawler / Scraping",
                    "severity": "WARNING",
                    "affected_entity": f"User {uid}",
                    "description": f"Disproportionate search requests ({searches} queries) with zero video playback initiation.",
                    "metric_value": f"{searches} queries : 0 plays",
                    "recommended_action": "Enforce rate limiting on search endpoint for this session."
                })

        # Default fallback positive health record if clean
        if not anomalies:
            anomalies.append({
                "anomaly_id": "ANO-HEALTH-OK",
                "category": "System Health",
                "severity": "INFO",
                "affected_entity": "Global Streaming Fleet",
                "description": "All streaming metrics, QoS buffering ratios, and concurrent session checks are within nominal limits.",
                "metric_value": "Nominal",
                "recommended_action": "Continue passive telemetry monitoring."
            })

        return anomalies

    def get_summary_metrics(self) -> Dict[str, Any]:
        """Provides high-level anomaly stats for dashboard badges."""
        anomalies = self.analyze_anomalies()
        crit = sum(1 for a in anomalies if a["severity"] == "CRITICAL")
        high = sum(1 for a in anomalies if a["severity"] in ["HIGH", "WARNING"])
        med = sum(1 for a in anomalies if a["severity"] == "MEDIUM")

        return {
            "total_anomalies": len(anomalies) if anomalies[0]["anomaly_id"] != "ANO-HEALTH-OK" else 0,
            "critical_count": crit,
            "warning_count": high,
            "medium_count": med,
            "fleet_status": "Healthy" if crit == 0 else "Action Required"
        }


# Global Singleton
anomaly_radar = StreamingAnomalyDetector()
