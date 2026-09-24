"""
DataStream AI - dbt Model & Transformation Runner
Executes dbt-style staging views, marts, and schema test assertions
directly against the active PostgreSQL or SQLite database.
Ensures zero-downtime transformations whether dbt CLI is installed or not.
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import text
from database.postgres import get_db

logger = logging.getLogger("datastream.dbt_runner")


class DBTRunner:
    def __init__(self):
        self.db = get_db()

    def run_models(self) -> Dict[str, Any]:
        """Materializes staging views and mart tables."""
        results = {"views_created": [], "tables_created": [], "errors": []}

        try:
            with self.db.engine.begin() as conn:
                # 1. Create or replace stg_events
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS stg_events AS
                SELECT 
                    event_id,
                    user_id,
                    LOWER(TRIM(event_type)) AS event_type,
                    content_id,
                    COALESCE(watch_time_mins, 0) AS watch_time_mins,
                    device,
                    session_id,
                    timestamp,
                    ip_address
                FROM events
                WHERE user_id IS NOT NULL;
                """))
                results["views_created"].append("stg_events")

                # 2. Create or replace stg_users
                conn.execute(text("""
                CREATE VIEW IF NOT EXISTS stg_users AS
                SELECT 
                    user_id,
                    name,
                    country,
                    tier,
                    status,
                    registration_date
                FROM users
                WHERE user_id IS NOT NULL;
                """))
                results["views_created"].append("stg_users")

                # 3. Create fact_user_activity table
                conn.execute(text("DROP TABLE IF EXISTS fact_user_activity;"))
                conn.execute(text("""
                CREATE TABLE fact_user_activity AS
                SELECT 
                    u.user_id,
                    u.name,
                    u.country,
                    u.tier,
                    COALESCE(COUNT(e.event_id), 0) AS total_events,
                    COALESCE(SUM(CASE WHEN e.event_type = 'video_watch' THEN e.watch_time_mins ELSE 0 END), 0) AS total_watch_mins,
                    COUNT(DISTINCT e.content_id) AS distinct_content_consumed,
                    MAX(e.timestamp) AS last_active_at
                FROM stg_users u
                LEFT JOIN stg_events e ON u.user_id = e.user_id
                GROUP BY u.user_id, u.name, u.country, u.tier;
                """))
                results["tables_created"].append("fact_user_activity")

                # 4. Create analytics_daily_usage table
                conn.execute(text("DROP TABLE IF EXISTS analytics_daily_usage;"))
                conn.execute(text("""
                CREATE TABLE analytics_daily_usage AS
                SELECT 
                    DATE(timestamp) AS usage_date,
                    COUNT(DISTINCT user_id) AS daily_active_users,
                    COUNT(event_id) AS total_events_logged,
                    SUM(CASE WHEN event_type = 'video_watch' THEN watch_time_mins ELSE 0 END) AS total_daily_watch_mins,
                    COUNT(CASE WHEN event_type = 'subscription' THEN 1 END) AS new_subscriptions_started
                FROM stg_events
                GROUP BY DATE(timestamp);
                """))
                results["tables_created"].append("analytics_daily_usage")

        except Exception as e:
            logger.error("dbt transformation execution error: %s", e)
            results["errors"].append(str(e))

        return results

    def run_tests(self) -> Dict[str, Any]:
        """Runs dbt-style assertions: not_null, unique, accepted_values."""
        test_results = []

        # Test 1: users unique user_id
        res1 = self.db.query("SELECT user_id, COUNT(*) as cnt FROM users GROUP BY user_id HAVING COUNT(*) > 1")
        test_results.append({
            "test_name": "unique_users_user_id",
            "model": "users",
            "status": "PASS" if res1.empty else "FAIL",
            "failures": len(res1)
        })

        # Test 2: users not_null user_id
        res2 = self.db.query("SELECT COUNT(*) as cnt FROM users WHERE user_id IS NULL")
        cnt2 = int(res2["cnt"].iloc[0]) if not res2.empty else 0
        test_results.append({
            "test_name": "not_null_users_user_id",
            "model": "users",
            "status": "PASS" if cnt2 == 0 else "FAIL",
            "failures": cnt2
        })

        # Test 3: events accepted_values
        res3 = self.db.query("""
        SELECT COUNT(*) as cnt FROM events 
        WHERE event_type NOT IN ('user_login', 'content_view', 'video_watch', 'search', 'add_to_watchlist', 'subscription', 'logout')
        """)
        cnt3 = int(res3["cnt"].iloc[0]) if not res3.empty else 0
        test_results.append({
            "test_name": "accepted_values_events_event_type",
            "model": "events",
            "status": "PASS" if cnt3 == 0 else "FAIL",
            "failures": cnt3
        })

        all_passed = all(t["status"] == "PASS" for t in test_results)
        return {"passed": all_passed, "tests": test_results}


dbt_runner = DBTRunner()
