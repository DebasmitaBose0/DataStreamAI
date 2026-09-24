-- Staging view: stg_users
-- Normalizes user profiles and redacts raw email identifiers

WITH raw_users AS (
    SELECT 
        user_id,
        name,
        country,
        tier,
        status,
        registration_date
    FROM {{ source('datastream', 'users') }}
    WHERE user_id IS NOT NULL
)
SELECT * FROM raw_users;
