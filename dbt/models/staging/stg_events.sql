-- Staging view: stg_events
-- Cleanses raw event records and casts types

WITH raw_events AS (
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
    FROM {{ source('datastream', 'events') }}
    WHERE user_id IS NOT NULL
)
SELECT * FROM raw_events;
