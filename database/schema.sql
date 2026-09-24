-- DataStream AI Relational Schema
-- Standard ANSI SQL compatible with both PostgreSQL and SQLite

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    phone VARCHAR(50),
    country VARCHAR(50),
    registration_date DATE NOT NULL,
    tier VARCHAR(50) DEFAULT 'Basic',
    status VARCHAR(50) DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS content (
    content_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(150) NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    genre VARCHAR(50) NOT NULL,
    release_year INTEGER,
    duration_mins INTEGER,
    rating NUMERIC(3, 1),
    director VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS subscriptions (
    sub_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    plan_name VARCHAR(50) NOT NULL,
    monthly_price NUMERIC(6, 2) NOT NULL,
    start_date DATE NOT NULL,
    renewal_date DATE,
    status VARCHAR(50) DEFAULT 'active',
    payment_method VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    event_type VARCHAR(50) NOT NULL,
    content_id VARCHAR(50),
    watch_time_mins INTEGER DEFAULT 0,
    device VARCHAR(50),
    session_id VARCHAR(100),
    timestamp TIMESTAMP NOT NULL,
    ip_address VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (content_id) REFERENCES content(content_id)
);

CREATE TABLE IF NOT EXISTS watch_history (
    history_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    content_id VARCHAR(50) NOT NULL,
    total_watch_time_mins INTEGER DEFAULT 0,
    completion_percentage NUMERIC(5, 2) DEFAULT 0.0,
    last_watched_at TIMESTAMP NOT NULL,
    completed BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (content_id) REFERENCES content(content_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_content ON events(content_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_watch_history_user ON watch_history(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id);
