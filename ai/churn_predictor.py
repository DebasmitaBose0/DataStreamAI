"""
DataStream AI - Subscriber Churn Prediction & Retention Engine
Analyzes streaming behavioral signals, engagement frequency, watch duration decay,
and error frictions to predict subscriber churn risk and recommend retention actions.
"""

from datetime import datetime
import pandas as pd
import numpy as np
from database.postgres import get_db


class ChurnPredictor:
    def __init__(self):
        self.db = get_db()

    def _extract_user_features(self) -> pd.DataFrame:
        """Aggregates multi-source behavioral signals per user."""
        users_df = self.db.query("SELECT * FROM users;")
        events_df = self.db.query("SELECT * FROM events;")
        subs_df = self.db.query("SELECT * FROM subscriptions;")

        if users_df.empty:
            return pd.DataFrame()

        # Parse event timestamps
        if not events_df.empty and "timestamp" in events_df.columns:
            events_df["dt"] = pd.to_datetime(events_df["timestamp"], errors="coerce")
            max_global_dt = events_df["dt"].max()
            if pd.isna(max_global_dt):
                max_global_dt = datetime.now()
        else:
            max_global_dt = datetime.now()

        features = []

        for _, u in users_df.iterrows():
            uid = u["user_id"]
            u_events = events_df[events_df["user_id"] == uid] if not events_df.empty else pd.DataFrame()
            u_subs = subs_df[subs_df["user_id"] == uid] if not subs_df.empty else pd.DataFrame()

            # Engagement metrics
            total_events = len(u_events)
            watch_events = u_events[u_events["event_type"] == "video_watch"] if total_events > 0 else pd.DataFrame()
            watch_count = len(watch_events)
            total_watch_time = watch_events["watch_time_mins"].sum() if watch_count > 0 else 0
            distinct_content = u_events["content_id"].dropna().nunique() if total_events > 0 else 0

            # Error / Friction count
            error_events = u_events[u_events["event_type"].isin(["buffering", "error", "playback_failed"])]
            error_count = len(error_events)

            # Inactivity calculation
            if total_events > 0 and not u_events["dt"].dropna().empty:
                last_active = u_events["dt"].max()
                days_inactive = max(0, (max_global_dt - last_active).days)
            else:
                days_inactive = 30  # Default dormant penalty

            tier = u.get("tier", "Basic")
            sub_status = u_subs.iloc[0].get("status", "active") if not u_subs.empty else "active"

            # Compute Risk Score (Heuristic ML Model)
            risk_score = 15.0  # Base natural churn propensity

            # Inactivity factor (up to +40)
            if days_inactive > 20:
                risk_score += 40.0
            elif days_inactive > 10:
                risk_score += 25.0
            elif days_inactive > 5:
                risk_score += 10.0
            else:
                risk_score -= 10.0  # Active recently

            # Watch time factor (up to +25 for very low watch time, -20 for high)
            if watch_count == 0 or total_watch_time < 30:
                risk_score += 25.0
            elif total_watch_time > 150:
                risk_score -= 20.0
            elif total_watch_time > 60:
                risk_score -= 10.0

            # Error / friction penalty (+15 if multiple errors)
            if error_count >= 2:
                risk_score += 15.0
            elif error_count == 1:
                risk_score += 8.0

            # Content diversity bonus (-10 if diversified)
            if distinct_content >= 3:
                risk_score -= 10.0

            # Tier weight: Basic users churn faster than Premium
            if tier == "Basic":
                risk_score += 10.0
            elif tier == "Premium":
                risk_score -= 8.0

            if sub_status in ["cancelled", "expired"]:
                risk_score = 98.0

            # Clamp between 2% and 98%
            risk_score = float(np.clip(risk_score, 2.0, 98.0))

            # Classification
            if risk_score >= 65.0:
                risk_level = "High"
                risk_color = "#ef4444"
            elif risk_score >= 35.0:
                risk_level = "Medium"
                risk_color = "#f59e0b"
            else:
                risk_level = "Low"
                risk_color = "#10b981"

            # Primary risk factors
            risk_factors = []
            if days_inactive >= 10:
                risk_factors.append(f"Dormant: {days_inactive} days since last stream")
            if total_watch_time < 45:
                risk_factors.append(f"Low engagement: Only {int(total_watch_time)} mins watched")
            if error_count > 0:
                risk_factors.append(f"Streaming friction: {error_count} playback issue(s) reported")
            if tier == "Basic" and risk_score > 40:
                risk_factors.append("Basic tier subscriber with low catalog discovery")
            if not risk_factors:
                risk_factors.append("Healthy engagement patterns across catalog")

            # Retention recommendations
            if risk_level == "High":
                retention_action = "🚨 Immediate Win-back: Send 25% discount voucher + highlight trending new releases"
            elif risk_level == "Medium":
                retention_action = "⚡ Re-engagement Push: Recommend personalized watchlist based on past genres"
            else:
                retention_action = "💎 Loyalty Nurture: Offer early access to 4K / VIP screenings"

            features.append({
                "user_id": uid,
                "name": u.get("name", "Unknown"),
                "email": u.get("email", ""),
                "country": u.get("country", ""),
                "tier": tier,
                "days_inactive": days_inactive,
                "total_watch_time_mins": int(total_watch_time),
                "watch_count": watch_count,
                "error_count": error_count,
                "distinct_content": distinct_content,
                "churn_risk_score": round(risk_score, 1),
                "risk_level": risk_level,
                "risk_color": risk_color,
                "risk_factors": " • ".join(risk_factors),
                "retention_action": retention_action
            })

        return pd.DataFrame(features)

    def predict_all_users(self) -> pd.DataFrame:
        """Returns full fleet risk predictions sorted by highest churn risk."""
        df = self._extract_user_features()
        if not df.empty:
            return df.sort_values(by="churn_risk_score", ascending=False).reset_index(drop=True)
        return df

    def predict_user(self, user_id: str) -> dict:
        """Returns detailed risk prediction for a single user."""
        df = self.predict_all_users()
        match = df[df["user_id"] == user_id]
        if not match.empty:
            return match.iloc[0].to_dict()
        return {}

    def get_fleet_summary(self) -> dict:
        """Returns executive KPI metrics for user churn."""
        df = self.predict_all_users()
        if df.empty:
            return {"total_users": 0, "high_risk": 0, "medium_risk": 0, "low_risk": 0, "avg_risk": 0.0}

        return {
            "total_users": len(df),
            "high_risk": int((df["risk_level"] == "High").sum()),
            "medium_risk": int((df["risk_level"] == "Medium").sum()),
            "low_risk": int((df["risk_level"] == "Low").sum()),
            "avg_risk": round(float(df["churn_risk_score"].mean()), 1),
            "at_risk_mrr_impact": int((df["risk_level"] == "High").sum()) * 14.99  # Assuming average sub price
        }


# Global Singleton
churn_engine = ChurnPredictor()
