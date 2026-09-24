-- Mart: fact_user_activity
-- Aggregates user lifetime interactions, stream minutes, and preferred device

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),
users AS (
    SELECT * FROM {{ ref('stg_users') }}
),
user_summary AS (
    SELECT 
        user_id,
        COUNT(event_id) AS total_events,
        SUM(CASE WHEN event_type = 'video_watch' THEN watch_time_mins ELSE 0 END) AS total_watch_mins,
        COUNT(DISTINCT content_id) AS distinct_content_consumed,
        MAX(timestamp) AS last_active_at
    FROM events
    GROUP BY user_id
)
SELECT 
    u.user_id,
    u.name,
    u.country,
    u.tier,
    COALESCE(s.total_events, 0) AS total_events,
    COALESCE(s.total_watch_mins, 0) AS total_watch_mins,
    COALESCE(s.distinct_content_consumed, 0) AS distinct_content_consumed,
    s.last_active_at
FROM users u
LEFT JOIN user_summary s ON u.user_id = s.user_id;
