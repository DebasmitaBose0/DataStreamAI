-- ==========================================================
-- DataStream AI: Portfolio SQL Analytics Suite
-- Demonstrates: CTEs, Window Functions, Aggregate Joins, Subqueries
-- ==========================================================

-- 1. Daily Active Users (DAU) & Event Volumes
-- Tracks unique daily active users and total events
SELECT 
    DATE(timestamp) AS activity_date,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(event_id) AS total_events,
    ROUND(CAST(COUNT(event_id) AS FLOAT) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS events_per_user
FROM events
WHERE user_id IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY activity_date DESC;


-- 2. Most Watched Content (Ranked by Total Watch Time)
-- Demonstrates aggregation, inner joins, and sorting
SELECT 
    c.content_id,
    c.title,
    c.content_type,
    c.genre,
    COUNT(e.event_id) AS total_views,
    SUM(e.watch_time_mins) AS total_watch_time_mins,
    ROUND(AVG(e.watch_time_mins), 1) AS avg_watch_mins,
    c.duration_mins,
    ROUND((SUM(e.watch_time_mins) * 1.0 / NULLIF(c.duration_mins * COUNT(e.event_id), 0)) * 100, 1) AS estimated_avg_completion_pct
FROM content c
INNER JOIN events e ON c.content_id = e.content_id
WHERE e.event_type = 'video_watch'
GROUP BY c.content_id, c.title, c.content_type, c.genre, c.duration_mins
ORDER BY total_watch_time_mins DESC;


-- 3. Top Users by Activity (Window Functions: DENSE_RANK & Running Totals)
-- Ranks users based on watch time and activity diversity
WITH UserActivitySummary AS (
    SELECT 
        u.user_id,
        u.name,
        u.country,
        u.tier,
        COUNT(e.event_id) AS total_interactions,
        COALESCE(SUM(CASE WHEN e.event_type = 'video_watch' THEN e.watch_time_mins ELSE 0 END), 0) AS total_stream_mins,
        COUNT(DISTINCT e.content_id) AS unique_titles_viewed
    FROM users u
    LEFT JOIN events e ON u.user_id = e.user_id
    GROUP BY u.user_id, u.name, u.country, u.tier
)
SELECT 
    user_id,
    name,
    country,
    tier,
    total_interactions,
    total_stream_mins,
    unique_titles_viewed,
    DENSE_RANK() OVER (ORDER BY total_stream_mins DESC, total_interactions DESC) AS activity_rank
FROM UserActivitySummary
ORDER BY activity_rank ASC
LIMIT 15;


-- 4. Content Engagement by Genre & Content Type
SELECT 
    c.content_type,
    c.genre,
    COUNT(DISTINCT c.content_id) AS total_titles,
    COUNT(e.event_id) AS total_sessions,
    COALESCE(SUM(e.watch_time_mins), 0) AS cumulative_watch_mins,
    ROUND(AVG(e.watch_time_mins), 1) AS mean_session_duration
FROM content c
LEFT JOIN events e ON c.content_id = e.content_id AND e.event_type = 'video_watch'
GROUP BY c.content_type, c.genre
ORDER BY cumulative_watch_mins DESC;


-- 5. Subscription Plan Breakdown & Monthly Recurring Revenue (MRR)
SELECT 
    s.plan_name,
    COUNT(s.sub_id) AS subscriber_count,
    ROUND(SUM(s.monthly_price), 2) AS total_monthly_revenue,
    ROUND(AVG(s.monthly_price), 2) AS avg_plan_price,
    ROUND(COUNT(s.sub_id) * 100.0 / (SELECT COUNT(*) FROM subscriptions), 1) AS plan_share_pct
FROM subscriptions s
WHERE s.status = 'active'
GROUP BY s.plan_name
ORDER BY total_monthly_revenue DESC;


-- 6. Event Funnel & Distribution by Device
SELECT 
    e.device,
    e.event_type,
    COUNT(e.event_id) AS event_count,
    ROUND(COUNT(e.event_id) * 100.0 / SUM(COUNT(e.event_id)) OVER (PARTITION BY e.device), 1) AS device_percentage
FROM events e
WHERE e.device IS NOT NULL AND e.device != ''
GROUP BY e.device, e.event_type
ORDER BY e.device, event_count DESC;


-- 7. User Retention Cohort Style Analysis (CTE + Window Analysis)
WITH FirstSeenCohort AS (
    SELECT 
        user_id,
        MIN(DATE(timestamp)) AS cohort_date
    FROM events
    WHERE user_id IS NOT NULL
    GROUP BY user_id
),
UserActivityDaily AS (
    SELECT DISTINCT
        user_id,
        DATE(timestamp) AS active_date
    FROM events
    WHERE user_id IS NOT NULL
)
SELECT 
    f.cohort_date,
    COUNT(DISTINCT f.user_id) AS cohort_size,
    COUNT(DISTINCT CASE WHEN a.active_date = f.cohort_date THEN a.user_id END) AS day_0_active,
    COUNT(DISTINCT CASE WHEN a.active_date > f.cohort_date THEN a.user_id END) AS retained_subsequent_days
FROM FirstSeenCohort f
LEFT JOIN UserActivityDaily a ON f.user_id = a.user_id
GROUP BY f.cohort_date
ORDER BY f.cohort_date;
