-- Mart: analytics_daily_usage
-- Daily aggregate usage metrics for dashboard and reporting

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
)
SELECT 
    DATE(timestamp) AS usage_date,
    COUNT(DISTINCT user_id) AS daily_active_users,
    COUNT(event_id) AS total_events_logged,
    SUM(CASE WHEN event_type = 'video_watch' THEN watch_time_mins ELSE 0 END) AS total_daily_watch_mins,
    COUNT(CASE WHEN event_type = 'subscription' THEN 1 END) AS new_subscriptions_started
FROM events
GROUP BY DATE(timestamp)
ORDER BY usage_date DESC;
