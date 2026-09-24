"""
DataStream AI - Production-Grade Streamlit Observability & Analytics Dashboard
"From Raw Data to Intelligent Insights"
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from database.postgres import get_db
from database.mongodb import get_mongo
from pipeline.ingestion import ingestion_engine
from pipeline.cleaning import cleaner
from pipeline.validation import dq_engine
from pipeline.schema_drift import drift_detector
from pipeline.etl_pipeline import etl_runner
from pipeline.dbt_runner import dbt_runner
from pipeline.privacy import mask_dataframe, mask_email, mask_phone, mask_ip, detect_pii_columns, get_governance_principles
from kafka.producer import producer
from kafka.consumer import consumer
from ai.ask_data import ask_data_engine
from ai.rag import rag_engine
from ai.churn_predictor import churn_engine
from ai.recommendation import content_recommender
from pipeline.anomaly_detector import anomaly_radar

# Page Configuration
st.set_page_config(
    page_title="DataStream AI | Data Engineering & AI Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom High-End Modern Styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background-color: #0b0f19;
        color: #e2e8f0;
    }
    
    /* Top Header Card */
    .hero-container {
        background: linear-gradient(135deg, rgba(14, 23, 42, 0.9) 0%, rgba(30, 41, 59, 0.7) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 14px;
        padding: 24px 28px;
        margin-bottom: 24px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
    }
    
    .hero-title {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 4px;
    }
    
    .hero-subtitle {
        color: #94a3b8;
        font-size: 1.05rem;
        font-weight: 400;
        letter-spacing: 0.3px;
    }
    
    /* Metric Glass Cards */
    .metric-card {
        background: rgba(17, 24, 39, 0.85);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 12px;
        backdrop-filter: blur(8px);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .metric-card:hover {
        border-color: rgba(56, 189, 248, 0.4);
        transform: translateY(-2px);
    }
    
    .metric-label {
        color: #94a3b8;
        font-size: 0.85rem;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 0.8px;
    }
    
    .metric-value {
        color: #f8fafc;
        font-size: 1.85rem;
        font-weight: 700;
        margin-top: 4px;
    }
    
    .metric-delta {
        font-size: 0.8rem;
        font-weight: 500;
        margin-top: 2px;
    }
    
    .status-badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .status-green { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(52, 211, 153, 0.3); }
    .status-blue { background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); }
    .status-yellow { background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(251, 191, 36, 0.3); }
    
    /* Code blocks and pre elements */
    pre, code {
        background-color: #0f172a !important;
        border-radius: 6px;
    }
</style>
""", unsafe_allow_html=True)


# Initialize Session State
if "initialized" not in st.session_state:
    st.session_state.initialized = True
    # Initial data load if tables are empty
    db = get_db()
    counts = db.get_table_counts()
    if counts.get("events", 0) == 0:
        etl_runner.run_pipeline()

db = get_db()
mongo = get_mongo()

# ----------------------------------------------------
# SIDEBAR: Infrastructure & Service Observability
# ----------------------------------------------------
with st.sidebar:
    st.markdown("### ⚡ **DataStream AI**")
    st.caption("Engineered by Debasmita | Full-Stack Pipeline")
    st.divider()

    st.markdown("#### 🌐 **Infrastructure Health**")
    
    # Database status
    db_type = db.db_type
    if "PostgreSQL" in db_type:
        st.markdown(f"**Relational DB:** <span class='status-badge status-green'>{db_type}</span>", unsafe_allow_html=True)
    else:
        st.markdown(f"**Relational DB:** <span class='status-badge status-yellow'>{db_type}</span>", unsafe_allow_html=True)
    st.caption("Auto-resilient failover active")

    # Document Store status
    mongo_status = mongo.storage_mode
    if "Live" in mongo_status:
        st.markdown(f"**Document Store:** <span class='status-badge status-green'>{mongo_status}</span>", unsafe_allow_html=True)
    else:
        st.markdown(f"**Document Store:** <span class='status-badge status-blue'>Local JSON Fallback</span>", unsafe_allow_html=True)

    # Kafka Status
    kafka_status = producer.mode
    if "Cluster" in kafka_status:
        st.markdown(f"**Streaming Bus:** <span class='status-badge status-green'>{kafka_status}</span>", unsafe_allow_html=True)
    else:
        st.markdown(f"**Streaming Bus:** <span class='status-badge status-blue'>Local Stream Simulator</span>", unsafe_allow_html=True)

    # AI Engine status
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key and len(gemini_key) > 10:
        st.markdown("**AI / RAG Model:** <span class='status-badge status-green'>Gemini 1.5 Flash</span>", unsafe_allow_html=True)
    else:
        st.markdown("**AI / RAG Model:** <span class='status-badge status-blue'>Local TF-IDF Vectorizer</span>", unsafe_allow_html=True)

    st.divider()
    st.markdown("#### ⚙️ **Quick Pipeline Actions**")
    if st.button("🚀 Run Full ETL Pipeline", use_container_width=True):
        with st.spinner("Executing Extract -> Transform -> Load..."):
            res = etl_runner.run_pipeline()
            st.success(f"ETL Completed: {res['metrics'].get('total_extracted', 0)} records processed!")
            st.rerun()

    if st.button("🔄 Clear & Re-Initialize DB", use_container_width=True):
        with st.spinner("Resetting warehouse tables..."):
            etl_runner.run_pipeline(force_recreate_schema=True)
            st.success("Warehouse refreshed!")
            st.rerun()

    st.divider()
    st.markdown("ℹ️ *Portfolio Note: Built with automatic graceful local fallbacks. All services execute 100% locally out-of-the-box.*")


# ----------------------------------------------------
# MAIN HERO HEADER
# ----------------------------------------------------
st.markdown("""
<div class="hero-container">
    <div class="hero-title">DataStream AI Platform</div>
    <div class="hero-subtitle">“From Raw Data to Intelligent Insights” — End-to-End OTT Data Engineering, Observability & AI Suite</div>
</div>
""", unsafe_allow_html=True)


# Tabs Definition
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12 = st.tabs([
    "📊 Overview",
    "📥 Ingestion",
    "🛡️ Data Quality",
    "⚡ SQL Analytics",
    "📡 Event Stream",
    "🤖 Ask Your Data",
    "📚 Knowledge Base",
    "🔄 Pipeline Monitor",
    "🔒 Privacy & Governance",
    "🎯 Churn & AI Retention",
    "🎬 Content Recommender",
    "🚨 Streaming Anomaly Radar"
])


# ====================================================
# TAB 1: OVERVIEW
# ====================================================
with tab1:
    st.markdown("### 📈 Platform Executive Summary")
    
    # Calculate real dynamic numbers from database and raw files
    tbl_counts = db.get_table_counts()
    events_df = db.query("SELECT * FROM events;")
    users_df = db.query("SELECT * FROM users;")
    content_df = db.query("SELECT * FROM content;")
    subs_df = db.query("SELECT * FROM subscriptions;")
    
    latest_run = etl_runner.get_latest_run()
    dq_summary = dq_engine.validate_events(events_df) if not events_df.empty else {"dq_score": 100, "duplicates": 0}

    # Top KPI Metrics Row
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Active Users</div>
            <div class="metric-value">{len(users_df)}</div>
            <div class="metric-delta" style="color: #34d399;">● 100% Cleansed</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Events Processed</div>
            <div class="metric-value">{len(events_df)}</div>
            <div class="metric-delta" style="color: #38bdf8;">Across {len(events_df['event_type'].unique()) if not events_df.empty else 0} Actions</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Data Quality Score</div>
            <div class="metric-value">{dq_summary.get('dq_score', 95.0)}%</div>
            <div class="metric-delta" style="color: #34d399;">● High Fidelity</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Catalog Titles</div>
            <div class="metric-value">{len(content_df)}</div>
            <div class="metric-delta" style="color: #c084fc;">Movies & Series</div>
        </div>
        """, unsafe_allow_html=True)
    with c5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Pipeline Status</div>
            <div class="metric-value" style="font-size: 1.45rem; color: #34d399;">{latest_run.get('status', 'HEALTHY') if latest_run else 'READY'}</div>
            <div class="metric-delta" style="color: #94a3b8;">Latency: {latest_run.get('duration_seconds', 0.8) if latest_run else 0.5}s</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("#### 🏗️ **Platform Data Flow Architecture**")
    st.info(
        "**Raw Event Telemetry & CSV Sources** ➔ "
        "**Ingestion Engine** (Schema Validation) ➔ "
        "**Data Quality & Cleaning** (Deduplication, Type Enforcement, PII Redaction) ➔ "
        "**Multi-Model Storage** (Relational PostgreSQL/SQLite + Document MongoDB/JSON) ➔ "
        "**SQL Analytics & dbt Marts** ➔ "
        "**AI / RAG & Observability Dashboards**"
    )

    # Overview Visualizations
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        if not events_df.empty:
            type_counts = events_df["event_type"].value_counts().reset_index()
            type_counts.columns = ["Event Type", "Count"]
            fig1 = px.bar(
                type_counts,
                x="Event Type",
                y="Count",
                title="Event Volume Distribution by User Action",
                color="Count",
                color_continuous_scale="Viridis",
                template="plotly_dark"
            )
            fig1.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320)
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.warning("No events data to display. Run ETL pipeline.")

    with col_chart2:
        if not events_df.empty:
            dev_counts = events_df["device"].value_counts().reset_index()
            dev_counts.columns = ["Device", "Count"]
            fig2 = px.pie(
                dev_counts,
                values="Count",
                names="Device",
                title="Platform Traffic by Client Device",
                hole=0.45,
                template="plotly_dark",
                color_discrete_sequence=px.colors.sequential.Teal
            )
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320)
            st.plotly_chart(fig2, use_container_width=True)


# ====================================================
# TAB 2: DATA INGESTION
# ====================================================
with tab2:
    st.markdown("### 📥 Ingestion & Pre-Processing Engine")
    st.caption("Inspect raw CSV/streaming datasets, schema profiles, and ingestion audit statistics.")

    ds_col1, ds_col2 = st.columns([1, 2])
    with ds_col1:
        selected_dataset = st.selectbox(
            "Select Platform Dataset to Inspect:",
            ["events", "users", "content", "subscriptions"]
        )
        csv_file_path = BASE_DIR / "data" / f"{selected_dataset}.csv"

        st.markdown("#### 📁 **Dataset Metadata**")
        if csv_file_path.exists():
            st.write(f"- **File Path:** `data/{selected_dataset}.csv`")
            st.write(f"- **File Size:** `{csv_file_path.stat().st_size} bytes`")
            st.write(f"- **Expected Schema Registered:** ✅ Yes")
        
        st.divider()
        st.markdown("#### 📤 **Custom File Ingestion Demo**")
        uploaded_file = st.file_uploader("Upload custom CSV to ingest:", type=["csv"])
        if uploaded_file is not None:
            temp_df = pd.read_csv(uploaded_file)
            st.success(f"Uploaded {len(temp_df)} records!")
            if st.button("Trigger Ingestion Audit"):
                temp_audit = dq_engine.validate_events(temp_df) if "event" in selected_dataset else dq_engine.validate_users(temp_df)
                st.json({k: v for k, v in temp_audit.items() if k not in ["invalid_mask", "error_samples"]})

    with ds_col2:
        if csv_file_path.exists():
            df_raw = pd.read_csv(csv_file_path)
            st.markdown(f"#### 🔍 **Raw Data Preview: `{selected_dataset}` ({len(df_raw)} Records)**")
            st.dataframe(df_raw.head(10), use_container_width=True)

            # Ingestion Audit for selected dataset
            st.markdown("#### 📊 **Pre-Ingestion Quality & Drift Audit**")
            drift = drift_detector.check_drift(selected_dataset, df_raw)
            
            c_a, c_b, c_c = st.columns(3)
            c_a.metric("Total Records", len(df_raw))
            c_b.metric("Duplicates in Raw", int(df_raw.duplicated().sum()))
            c_c.metric("Missing Values", int(df_raw.isnull().sum().sum()))

            if drift["warnings"]:
                st.warning("⚠️ Schema Warnings Encountered:")
                for w in drift["warnings"]:
                    st.write(f"- {w}")
            else:
                st.success("✅ Zero schema drift detected against baseline definition.")


# ====================================================
# TAB 3: DATA QUALITY ENGINE
# ====================================================
with tab3:
    st.markdown("### 🛡️ Reusable Data Quality & Validation Engine")
    st.caption("Calculates real dynamic health scores and catches intentional anomalies from raw streaming inputs.")

    # Run actual audit on raw events.csv (which contains our intentional dirty test rows!)
    raw_events_path = BASE_DIR / "data" / "events.csv"
    raw_events_df = pd.read_csv(raw_events_path)
    audit = dq_engine.validate_events(raw_events_df)

    # DQ Cards Row
    dq1, dq2, dq3, dq4, dq5 = st.columns(5)
    dq1.metric("Total Raw Records", audit["total_records"])
    dq2.metric("Valid Records", audit["valid_records"], delta=f"{(audit['valid_records']/audit['total_records']*100):.1f}%")
    dq3.metric("Invalid Records", audit["invalid_records"], delta="-Rejected", delta_color="inverse")
    dq4.metric("Duplicates Caught", audit["duplicates"])
    dq5.metric("Data Quality Score", f"{audit['dq_score']}%")

    st.divider()

    # Detailed Anomaly breakdown
    col_breakdown, col_log = st.columns([1, 1])
    with col_breakdown:
        st.markdown("#### 🔬 **Granular Quality Rule Violations**")
        rule_data = pd.DataFrame([
            {"Rule": "Duplicate Event IDs", "Violations": audit["duplicates"], "Status": "FAIL" if audit["duplicates"] > 0 else "PASS"},
            {"Rule": "Missing Mandatory User IDs", "Violations": audit["invalid_user_ids"], "Status": "FAIL" if audit["invalid_user_ids"] > 0 else "PASS"},
            {"Rule": "Invalid Event Types", "Violations": audit["invalid_event_types"], "Status": "FAIL" if audit["invalid_event_types"] > 0 else "PASS"},
            {"Rule": "Negative / Malformed Watch Time", "Violations": audit["invalid_numerics"], "Status": "FAIL" if audit["invalid_numerics"] > 0 else "PASS"},
            {"Rule": "Unparseable Timestamps", "Violations": audit["invalid_timestamps"], "Status": "FAIL" if audit["invalid_timestamps"] > 0 else "PASS"},
        ])
        st.dataframe(rule_data, use_container_width=True, hide_index=True)

        st.markdown("#### ⚠️ **Schema Drift Engine Alerts**")
        if audit["schema_warnings"]:
            for warn in audit["schema_warnings"]:
                st.error(warn)
        else:
            st.success("Baseline schema verified: All expected columns, types, and constraints valid.")

    with col_log:
        st.markdown("#### 🚨 **Detected Malformed Records Log (Actual Data)**")
        st.caption("Rows rejected by the cleaning boundary before landing in operational database:")
        if audit["error_samples"]:
            st.dataframe(pd.DataFrame(audit["error_samples"]), use_container_width=True, hide_index=True)
        else:
            st.info("No malformed records detected.")


# ====================================================
# TAB 4: SQL ANALYTICS
# ====================================================
with tab4:
    st.markdown("### ⚡ Advanced SQL Analytics Suite")
    st.caption("Execute analytical SQL queries featuring CTEs, Window Functions, and Multi-Table Joins against the operational warehouse.")

    PRE_BUILT_QUERIES = {
        "1. Daily Active Users (DAU) & Event Volumes": """
SELECT 
    DATE(timestamp) AS activity_date,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(event_id) AS total_events,
    ROUND(CAST(COUNT(event_id) AS FLOAT) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS events_per_user
FROM events
WHERE user_id IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY activity_date DESC;
""",
        "2. Most Watched Content (Ranked by Watch Duration)": """
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
""",
        "3. Top Users by Activity (Window Function: DENSE_RANK)": """
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
LIMIT 10;
""",
        "4. Content Engagement by Genre & Content Type": """
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
""",
        "5. Subscriptions Breakdown & Monthly Recurring Revenue (MRR)": """
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
""",
        "6. Event Funnel by Client Device": """
SELECT 
    e.device,
    e.event_type,
    COUNT(e.event_id) AS event_count
FROM events e
WHERE e.device IS NOT NULL AND e.device != ''
GROUP BY e.device, e.event_type
ORDER BY e.device, event_count DESC;
"""
    }

    selected_query_label = st.selectbox("Choose Pre-Built Analytics Query:", list(PRE_BUILT_QUERIES.keys()))
    query_to_run = PRE_BUILT_QUERIES[selected_query_label]

    with st.expander("📝 View / Edit SQL Query Text", expanded=False):
        custom_sql = st.text_area("SQL Statement:", query_to_run, height=180)
        if custom_sql:
            query_to_run = custom_sql

    # Execute SQL
    query_res_df = db.query(query_to_run)

    col_q1, col_q2 = st.columns([1, 1])
    with col_q1:
        st.markdown(f"#### 📊 Query Results ({len(query_res_df)} Rows)")
        st.dataframe(query_res_df, use_container_width=True)

    with col_q2:
        st.markdown("#### 📈 Dynamic Data Visualization")
        if not query_res_df.empty:
            cols = list(query_res_df.columns)
            if "total_watch_time_mins" in cols and "title" in cols:
                fig = px.bar(query_res_df.head(7), x="title", y="total_watch_time_mins", color="genre", title="Watch Time by Title (Mins)", template="plotly_dark")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
            elif "total_monthly_revenue" in cols and "plan_name" in cols:
                fig = px.pie(query_res_df, values="total_monthly_revenue", names="plan_name", title="MRR by Plan Type ($)", hole=0.4, template="plotly_dark")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
            elif "activity_rank" in cols and "name" in cols:
                fig = px.bar(query_res_df.head(7), x="name", y="total_stream_mins", color="tier", title="Top Streamers by Watch Duration", template="plotly_dark")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
            elif "activity_date" in cols:
                fig = px.line(query_res_df, x="activity_date", y="active_users", markers=True, title="Daily Active Users Over Time", template="plotly_dark")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Visual representation auto-adapts to selected analytical metric.")
        else:
            st.warning("Query produced empty result set.")


# ====================================================
# TAB 5: EVENT STREAM (KAFKA SIMULATOR)
# ====================================================
with tab5:
    st.markdown("### 📡 Real-Time Event Streaming & Kafka Bus")
    st.caption("Publishes streaming OTT user events to Kafka topic 'user-events' with validation and buffer monitoring.")

    k_col1, k_col2 = st.columns([1, 2])
    with k_col1:
        st.markdown("#### 🕹️ **Event Stream Controls**")
        st.write(f"- **Operating Mode:** `{producer.mode}`")
        st.write(f"- **Kafka Topic:** `{producer.topic}`")
        st.write(f"- **Buffer Depth:** `{len(consumer.consumed_events_log)} events`")

        st.divider()
        b_col1, b_col2 = st.columns(2)
        with b_col1:
            if st.button("⚡ Emit 1 Event", use_container_width=True):
                res = producer.emit_event()
                consumer.consume_from_local_buffer(max_items=1)
                st.success("Event dispatched & processed!")
                st.rerun()
        with b_col2:
            if st.button("🚀 Emit 10 Events", use_container_width=True):
                producer.emit_batch(count=10)
                consumer.consume_from_local_buffer(max_items=10)
                st.success("Burst of 10 events processed!")
                st.rerun()

        # Streaming Metrics
        stream_metrics = consumer.get_metrics()
        st.divider()
        st.metric("Total Events Consumed", stream_metrics["total_consumed"])
        st.metric("Valid Events", stream_metrics["valid_events"], delta=f"{stream_metrics['valid_rate']}% Valid")
        st.metric("Invalid Filtered", stream_metrics["invalid_events"])

    with k_col2:
        st.markdown("#### 📺 **Live Consumed Event Stream (Real-Time Feed)**")
        if consumer.consumed_events_log:
            stream_df = pd.DataFrame(consumer.consumed_events_log)
            st.dataframe(
                stream_df.tail(15)[["consumed_at", "event_id", "user_id", "event_type", "content_id", "watch_time_mins", "device", "is_valid", "validation_note"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No streaming events in current session. Click 'Emit 1 Event' or 'Emit 10 Events' to test live streaming!")


# ====================================================
# TAB 6: AI / ASK YOUR DATA
# ====================================================
with tab6:
    st.markdown("### 🤖 “Ask Your Data” Natural Language AI Layer")
    st.caption("Ask questions in plain English. The AI agent formulates analytical queries, executes against the warehouse, and delivers factual answers without hallucination.")

    preset_questions = [
        "Which content has the highest watch time?",
        "Which users are most active?",
        "What were the most common events today?",
        "Which content is trending?",
        "Summarize the current data quality status.",
        "What is our subscription MRR and breakdown?"
    ]

    selected_preset = st.selectbox("💡 Select a Sample Question or Type Your Own Below:", [""] + preset_questions)
    user_input = st.text_input("Ask a question about your OTT streaming data:", value=selected_preset)

    if st.button("🔍 Analyze with AI", use_container_width=True) or user_input:
        if user_input:
            with st.spinner("AI analyzing database..."):
                response = ask_data_engine.answer_query(user_input)

                st.markdown("#### 💡 **AI Response & Executive Takeaways:**")
                st.markdown(response["answer"])

                st.markdown("#### ⚙️ **Underlying SQL Executed Against Database:**")
                st.code(response["sql_query"], language="sql")

                if not response["data"].empty:
                    st.markdown("#### 📋 **Grounding Data Set:**")
                    st.dataframe(response["data"], use_container_width=True)


# ====================================================
# TAB 7: KNOWLEDGE BASE & RAG
# ====================================================
with tab7:
    st.markdown("### 📚 Content Catalog & Operations Knowledge Base (RAG)")
    st.caption("Semantic vector search over unstructured documentation, content catalog synopses, and platform guidelines.")

    rag_col1, rag_col2 = st.columns([1, 2])
    with rag_col1:
        st.markdown("#### 📑 **Indexed Documents**")
        for doc in rag_engine.indexed_documents:
            st.write(f"- 📄 `{doc}`")

        st.write(f"- **Total Chunks in Vector Index:** `{len(rag_engine.vector_engine.chunk_records)}`")
        st.write(f"- **Vector Engine:** `{rag_engine.vector_engine.engine_mode}`")

        st.divider()
        st.markdown("#### 📤 **Upload Document to Knowledge Base**")
        uploaded_doc = st.file_uploader("Upload .txt or .md file:", type=["txt", "md"])
        if uploaded_doc is not None:
            text_content = uploaded_doc.read().decode("utf-8")
            if st.button("Index Uploaded Document"):
                chunks_added = rag_engine.index_raw_text(text_content, doc_name=uploaded_doc.name)
                st.success(f"Indexed {chunks_added} chunks into vector space!")
                st.rerun()

    with rag_col2:
        st.markdown("#### 🔎 **Ask the Knowledge Base**")
        rag_query = st.text_input(
            "Query catalog & guidelines (e.g. 'Tell me about Cybernetic Dawn storyline', 'What are the subscription plans?'):",
            value="What are the subscription tiers and resolution differences?"
        )

        if st.button("Search & Generate Answer") or rag_query:
            with st.spinner("Searching semantic embeddings..."):
                rag_res = rag_engine.answer_question(rag_query, top_k=2)

                st.markdown(f"**Engine Used:** `{rag_res['engine_used']}`")
                st.markdown(rag_res["answer"])

                with st.expander("🔍 View Retrieved Semantic Chunks & Similarity Scores"):
                    for item in rag_res["retrieved_chunks"]:
                        st.markdown(f"**Source:** `{item['chunk']['metadata'].get('source')}` | **Similarity Score:** `{item['similarity_score']}`")
                        st.code(item['chunk']['text'], language="markdown")


# ====================================================
# TAB 8: PIPELINE MONITOR & RESILIENCE
# ====================================================
with tab8:
    st.markdown("### 🔄 Pipeline Orchestration & Self-Healing Resilience")
    st.caption("Trigger ETL workflows, inspect Airflow DAG architecture, run dbt tests, and test self-healing retry logic.")

    p_col1, p_col2 = st.columns([1, 1])
    with p_col1:
        st.markdown("#### 🧪 **Self-Healing & Resilience Test Prototype**")
        st.write(
            "Test how the pipeline automatically recovers when a transient error occurs during data extraction."
        )
        sim_fail = st.checkbox("Simulate Transient Network Timeout on Attempt 1", value=False)
        retry_max = st.slider("Max Retry Attempts:", min_value=1, max_value=5, value=3)

        if st.button("▶️ Execute Resilient ETL Pipeline", use_container_width=True):
            with st.spinner("Running ETL with retry supervisor..."):
                run_res = etl_runner.run_pipeline(max_retries=retry_max, simulate_failure_once=sim_fail)
                if run_res["status"] == "SUCCESS":
                    st.success(f"Pipeline succeeded after {run_res['retries_attempted']} retry recovery attempt(s)!")
                else:
                    st.error(f"Pipeline failed: {run_res['error_message']}")
                st.rerun()

        st.divider()
        st.markdown("#### 🛠️ **dbt Transformations & Data Tests**")
        if st.button("Run dbt Models & Quality Assertions", use_container_width=True):
            with st.spinner("Executing dbt staging views and mart transformations..."):
                dbt_models = dbt_runner.run_models()
                dbt_tests = dbt_runner.run_tests()
                st.success(f"Models materialized: {dbt_models['views_created'] + dbt_models['tables_created']}")
                if dbt_tests["passed"]:
                    st.info(f"All {len(dbt_tests['tests'])} dbt schema assertions passed (unique, not_null, accepted_values)!")
                st.dataframe(pd.DataFrame(dbt_tests["tests"]), use_container_width=True)

    with p_col2:
        st.markdown("#### 📜 **ETL Execution Log Stream**")
        latest = etl_runner.get_latest_run()
        if latest and "logs" in latest:
            log_df = pd.DataFrame(latest["logs"])
            st.dataframe(log_df, use_container_width=True, hide_index=True)
        else:
            st.info("No execution logs recorded yet.")

        st.markdown("#### 📦 **Warehouse Table Record Counts**")
        st.dataframe(pd.DataFrame(list(tbl_counts.items()), columns=["Table", "Record Count"]), use_container_width=True, hide_index=True)


# ====================================================
# TAB 9: PRIVACY & DATA GOVERNANCE
# ====================================================
with tab9:
    st.markdown("### 🔒 Privacy, PII Protection & Data Governance")
    st.caption("Demonstrates automated PII detection, redaction, data minimization, and DPDP-aware privacy controls.")

    gov = get_governance_principles()
    st.info(f"🛡️ **Compliance Notice:** {gov['disclaimer']}")

    # Interactive PII Redaction Demo
    st.markdown("#### 🧪 **Live PII Redaction Demonstration**")
    raw_users_p = BASE_DIR / "data" / "users.csv"
    if raw_users_p.exists():
        users_raw_df = pd.read_csv(raw_users_p).head(5)
        users_masked_df = mask_dataframe(users_raw_df)

        toggle_view = st.radio("Toggle Table View:", ["Masked PII (Safe for Analytics / Dashboard)", "Raw Storage View (Restricted Access)"], horizontal=True)

        if "Masked" in toggle_view:
            st.markdown("##### 🔒 Masked View (Emails & Phones Redacted)")
            st.dataframe(users_masked_df[["user_id", "name", "email", "phone", "country", "tier"]], use_container_width=True)
        else:
            st.markdown("##### ⚠️ Raw Ingestion View")
            st.dataframe(users_raw_df[["user_id", "name", "email", "phone", "country", "tier"]], use_container_width=True)

    st.markdown("#### 📋 **Data Governance & DPDP Architectural Pillars**")
    for item in gov["principles"]:
        with st.expander(f"📌 {item['pillar']}", expanded=True):
            st.write(item["detail"])

    st.markdown("#### 🔍 **Automated PII Column Detection**")
    detected = detect_pii_columns(users_raw_df)
    st.dataframe(pd.DataFrame(detected), use_container_width=True, hide_index=True)


# ====================================================
# TAB 10: SUBSCRIBER CHURN PREDICTION & AI RETENTION
# ====================================================
with tab10:
    st.markdown("### 🎯 Subscriber Churn Prediction & Retention AI")
    st.caption("AI-driven behavioral telemetry analyzing inactivity decay, QoS playback friction, and engagement velocity to forecast subscriber churn risk.")

    fleet_summary = churn_engine.get_fleet_summary()
    churn_df = churn_engine.predict_all_users()

    # KPI Metrics Row
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Tracked Subscribers</div>
            <div class="metric-value">{fleet_summary['total_users']}</div>
            <div class="metric-delta" style="color:#38bdf8;">Across all active tiers</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">High Churn Risk</div>
            <div class="metric-value" style="color:#ef4444;">{fleet_summary['high_risk']}</div>
            <div class="metric-delta" style="color:#ef4444;">Requires immediate retention</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Fleet Avg Churn Risk</div>
            <div class="metric-value">{fleet_summary['avg_risk']}%</div>
            <div class="metric-delta" style="color:#f59e0b;">Target: &lt; 25%</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">At-Risk Monthly Revenue</div>
            <div class="metric-value" style="color:#fbbf24;">${fleet_summary['at_risk_mrr_impact']:.2f}</div>
            <div class="metric-delta" style="color:#94a3b8;">Potential MRR exposure</div>
        </div>
        """, unsafe_allow_html=True)

    # Churn Visualizations
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.markdown("#### 📊 **Risk Level Distribution**")
        risk_counts = churn_df["risk_level"].value_counts().reset_index()
        risk_counts.columns = ["Risk Level", "Count"]
        color_map = {"Low": "#10b981", "Medium": "#f59e0b", "High": "#ef4444"}
        fig_risk = px.pie(
            risk_counts,
            names="Risk Level",
            values="Count",
            color="Risk Level",
            color_discrete_map=color_map,
            hole=0.45
        )
        fig_risk.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
        )
        st.plotly_chart(fig_risk, use_container_width=True)

    with col_chart2:
        st.markdown("#### 📉 **Engagement vs Churn Probability**")
        fig_scatter = px.scatter(
            churn_df,
            x="days_inactive",
            y="churn_risk_score",
            color="risk_level",
            size="total_watch_time_mins",
            hover_name="name",
            hover_data=["user_id", "tier", "total_watch_time_mins", "error_count"],
            color_discrete_map=color_map,
            labels={"days_inactive": "Days Since Last Stream", "churn_risk_score": "Churn Probability (%)"}
        )
        fig_scatter.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    # Individual Subscriber Deep-Dive
    st.markdown("#### 🔬 **Subscriber Diagnostic & AI Retention Strategy**")
    user_options = {f"{r['user_id']} - {r['name']} ({r['tier']})": r['user_id'] for _, r in churn_df.iterrows()}
    selected_label = st.selectbox("Select a Subscriber to Diagnose:", list(user_options.keys()))
    selected_uid = user_options[selected_label]
    user_pred = churn_engine.predict_user(selected_uid)

    if user_pred:
        d_col1, d_col2 = st.columns([1, 2])
        with d_col1:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=user_pred["churn_risk_score"],
                title={"text": "Churn Probability", "font": {"size": 18, "color": "#f8fafc"}},
                number={"suffix": "%", "font": {"color": user_pred["risk_color"], "size": 36}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": "#94a3b8"},
                    "bar": {"color": user_pred["risk_color"]},
                    "steps": [
                        {"range": [0, 35], "color": "rgba(16, 185, 129, 0.2)"},
                        {"range": [35, 65], "color": "rgba(245, 158, 11, 0.2)"},
                        {"range": [65, 100], "color": "rgba(239, 68, 68, 0.2)"}
                    ],
                    "threshold": {
                        "line": {"color": "white", "width": 3},
                        "thickness": 0.8,
                        "value": user_pred["churn_risk_score"]
                    }
                }
            ))
            fig_gauge.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=240,
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_gauge, use_container_width=True)

        with d_col2:
            st.markdown(f"##### 👤 **{user_pred['name']}** (`{user_pred['user_id']}`)")
            st.markdown(f"**Country:** {user_pred['country']} | **Plan Tier:** `{user_pred['tier']}` | **Risk Level:** <span style='color:{user_pred['risk_color']}; font-weight:700;'>{user_pred['risk_level']} Risk</span>", unsafe_allow_html=True)
            
            st.markdown("###### 🔍 **Key Driving Risk Factors:**")
            for factor in user_pred["risk_factors"].split(" • "):
                st.markdown(f"- ⚠️ {factor}")

            st.markdown("###### 💡 **Prescriptive AI Retention Action:**")
            st.info(user_pred["retention_action"])

    st.markdown("#### 📋 **Subscriber Fleet Risk Register**")
    st.dataframe(
        churn_df[["user_id", "name", "tier", "days_inactive", "total_watch_time_mins", "churn_risk_score", "risk_level", "retention_action"]],
        use_container_width=True,
        hide_index=True
    )


# ====================================================
# TAB 11: PERSONALIZED CONTENT RECOMMENDER
# ====================================================
with tab11:
    st.markdown("### 🎬 Personalized OTT Content Recommendation Engine")
    st.caption("Hybrid content-based and collaborative intelligence ranking movies & series by user taste profile, genre affinity, and director matching.")

    rec_users = db.query("SELECT user_id, name, tier FROM users;")
    if not rec_users.empty:
        r_user_map = {f"{r['user_id']} - {r['name']}": r['user_id'] for _, r in rec_users.iterrows()}
        selected_rec_label = st.selectbox("Select User Profile for Recommendations:", list(r_user_map.keys()), key="rec_user_select")
        rec_uid = r_user_map[selected_rec_label]

        profile = content_recommender.get_user_profile(rec_uid)
        
        # User Taste Profile summary
        p_c1, p_c2, p_c3 = st.columns(3)
        with p_c1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Watched Titles</div>
                <div class="metric-value">{profile['total_watched_count']}</div>
                <div class="metric-delta" style="color:#38bdf8;">Catalog discovery</div>
            </div>
            """, unsafe_allow_html=True)
        with p_c2:
            fav_g_str = ", ".join(profile['favorite_genres']) if profile['favorite_genres'] else "Diverse / Exploring"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Top Affinity Genres</div>
                <div class="metric-value" style="font-size:1.2rem; color:#a78bfa;">{fav_g_str}</div>
                <div class="metric-delta">Derived from watch history</div>
            </div>
            """, unsafe_allow_html=True)
        with p_c3:
            fav_d_str = ", ".join(profile['favorite_directors']) if profile['favorite_directors'] else "Various Auteurs"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Preferred Directors</div>
                <div class="metric-value" style="font-size:1.2rem; color:#34d399;">{fav_d_str}</div>
                <div class="metric-delta">Director affinity matching</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown(f"#### 🌟 **Top Recommendations for {selected_rec_label.split(' - ')[1]}**")
        recs = content_recommender.recommend_for_user(rec_uid, top_n=4)

        if recs:
            rec_cols = st.columns(len(recs))
            for i, rec in enumerate(recs):
                with rec_cols[i]:
                    st.markdown(f"""
                    <div style="background: rgba(30, 41, 59, 0.7); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 10px; padding: 16px; min-height: 250px;">
                        <span class="status-badge status-blue">{rec['content_type']}</span>
                        <span class="status-badge status-green" style="float:right;">{rec['match_score']}% Match</span>
                        <h4 style="margin-top: 12px; margin-bottom: 4px; color: #f8fafc;">{rec['title']}</h4>
                        <p style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 8px;"><b>Genre:</b> {rec['genre']} | ⭐ {rec['rating']}/10</p>
                        <p style="color: #64748b; font-size: 0.8rem; margin-bottom: 12px;"><b>Director:</b> {rec['director']}</p>
                        <div style="background: rgba(15, 23, 42, 0.9); padding: 8px 10px; border-radius: 6px; font-size: 0.78rem; color: #38bdf8;">
                            {rec['reason']}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        st.divider()

        # Item-to-Item Similarity Explorer
        st.markdown("#### 🔄 **Content-to-Content Similarity Radar**")
        catalog_df = db.query("SELECT content_id, title, genre FROM content;")
        if not catalog_df.empty:
            cat_map = {f"{r['title']} ({r['genre']})": r['content_id'] for _, r in catalog_df.iterrows()}
            selected_cat_label = st.selectbox("Pick a Title to Discover Similar Catalog Items:", list(cat_map.keys()))
            chosen_cid = cat_map[selected_cat_label]
            sims = content_recommender.get_similar_content(chosen_cid, top_n=4)

            if sims:
                sim_cols = st.columns(len(sims))
                for j, s in enumerate(sims):
                    with sim_cols[j]:
                        st.markdown(f"""
                        <div style="background: rgba(17, 24, 39, 0.6); border: 1px solid rgba(56, 189, 248, 0.2); border-radius: 8px; padding: 14px;">
                            <span class="status-badge status-green">⭐ {s['similarity_score']}% Similarity</span>
                            <h5 style="margin-top: 10px; color: #f8fafc;">{s['title']}</h5>
                            <p style="color: #94a3b8; font-size: 0.85rem;">Genre: {s['genre']}</p>
                            <p style="color: #fbbf24; font-size: 0.85rem;">IMDb: {s['rating']}/10</p>
                        </div>
                        """, unsafe_allow_html=True)


# ====================================================
# TAB 12: REAL-TIME STREAMING ANOMALY & FRAUD RADAR
# ====================================================
with tab12:
    st.markdown("### 🚨 Streaming Anomaly & Security Radar")
    st.caption("Active telemetry surveillance detecting concurrent stream credential fraud, playback QoS degradation, and automated web crawlers.")

    radar_summary = anomaly_radar.get_summary_metrics()
    anomalies = anomaly_radar.analyze_anomalies()

    # Metrics row
    a1, a2, a3, a4 = st.columns(4)
    with a1:
        fleet_color = "#10b981" if radar_summary["fleet_status"] == "Healthy" else "#ef4444"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Streaming Fleet Status</div>
            <div class="metric-value" style="color:{fleet_color};">{radar_summary['fleet_status']}</div>
            <div class="metric-delta">Global CDN health</div>
        </div>
        """, unsafe_allow_html=True)
    with a2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Active Anomalies</div>
            <div class="metric-value" style="color:#fbbf24;">{radar_summary['total_anomalies']}</div>
            <div class="metric-delta">Flagged in recent stream</div>
        </div>
        """, unsafe_allow_html=True)
    with a3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Critical Security Alerts</div>
            <div class="metric-value" style="color:#ef4444;">{radar_summary['critical_count']}</div>
            <div class="metric-delta">Credential fraud / Multi-IP</div>
        </div>
        """, unsafe_allow_html=True)
    with a4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">QoS Warnings</div>
            <div class="metric-value" style="color:#38bdf8;">{radar_summary['warning_count']}</div>
            <div class="metric-delta">Buffering / Error ratios</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("#### 📡 **Live Security & QoS Incident Radar**")
    for ano in anomalies:
        if ano["severity"] == "CRITICAL":
            badge_html = "<span style='background:rgba(239,68,68,0.2); color:#ef4444; border:1px solid #ef4444; border-radius:9999px; padding:3px 8px; font-size:0.75rem; font-weight:700;'>CRITICAL</span>"
        elif ano["severity"] in ["HIGH", "WARNING"]:
            badge_html = "<span style='background:rgba(245,158,11,0.2); color:#f59e0b; border:1px solid #f59e0b; border-radius:9999px; padding:3px 8px; font-size:0.75rem; font-weight:700;'>WARNING</span>"
        else:
            badge_html = "<span style='background:rgba(52,211,153,0.2); color:#34d399; border:1px solid #34d399; border-radius:9999px; padding:3px 8px; font-size:0.75rem; font-weight:700;'>INFO</span>"

        with st.container():
            st.markdown(f"""
            <div style="background: rgba(17, 24, 39, 0.7); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 8px; padding: 14px 18px; margin-bottom: 10px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                    <div>
                        {badge_html}
                        <span style="color: #94a3b8; font-size: 0.85rem; margin-left: 8px;"><code>{ano['anomaly_id']}</code> &nbsp;|&nbsp; <b>{ano['category']}</b></span>
                    </div>
                    <span style="color: #e2e8f0; font-size: 0.85rem; font-weight: 600;">Entity: {ano['affected_entity']}</span>
                </div>
                <p style="color: #cbd5e1; margin: 4px 0 6px 0; font-size: 0.92rem;">{ano['description']}</p>
                <div style="color: #38bdf8; font-size: 0.82rem;"><b>💡 Prescribed Action:</b> {ano['recommended_action']}</div>
            </div>
            """, unsafe_allow_html=True)

    # Streaming Event Distribution
    st.markdown("#### 📊 **Global Event Distribution & Playback Health**")
    events_df_anom = db.query("SELECT event_type, device FROM events;")
    if not events_df_anom.empty:
        col_anom1, col_anom2 = st.columns(2)
        with col_anom1:
            evt_counts = events_df_anom["event_type"].value_counts().reset_index()
            evt_counts.columns = ["Event Type", "Count"]
            fig_evt = px.bar(evt_counts, x="Event Type", y="Count", color="Event Type", title="Streaming Events by Action Type")
            fig_evt.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig_evt, use_container_width=True)

        with col_anom2:
            dev_counts = events_df_anom["device"].value_counts().reset_index()
            dev_counts.columns = ["Device", "Count"]
            fig_dev = px.pie(dev_counts, names="Device", values="Count", title="Device Hardware Fleet Share", hole=0.4)
            fig_dev.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dev, use_container_width=True)
