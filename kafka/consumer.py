"""
DataStream AI - Kafka Event Stream Consumer & Processor
Consumes events from Kafka topic or local buffer, performs real-time validation,
routes valid events to database & MongoDB, and tracks streaming observability metrics.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
from dotenv import load_dotenv

from database.postgres import get_db
from database.mongodb import get_mongo
from pipeline.validation import VALID_EVENT_TYPES
from pipeline.privacy import mask_ip
from kafka.producer import LOCAL_STREAM_BUFFER

load_dotenv()
logger = logging.getLogger("datastream.kafka.consumer")


class EventConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "user-events")
        self.db = get_db()
        self.mongo = get_mongo()

        self.consumed_count = 0
        self.valid_count = 0
        self.invalid_count = 0
        self.consumed_events_log: List[Dict[str, Any]] = []
        self.invalid_events_log: List[Dict[str, Any]] = []

    def validate_event(self, event: Dict[str, Any]) -> tuple:
        """Validates streaming event payload."""
        if not event.get("event_id"):
            return False, "Missing event_id"
        if not event.get("user_id"):
            return False, "Missing user_id"
        if event.get("event_type") not in VALID_EVENT_TYPES:
            return False, f"Invalid event_type '{event.get('event_type')}'"
        
        watch_time = event.get("watch_time_mins", 0)
        try:
            if int(watch_time) < 0:
                return False, "Negative watch_time_mins"
        except (ValueError, TypeError):
            return False, "Malformed watch_time_mins"

        return True, "Valid"

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Validates and routes a single streaming event."""
        self.consumed_count += 1
        is_valid, reason = self.validate_event(event)

        record_entry = {
            "consumed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event_id": event.get("event_id"),
            "user_id": event.get("user_id"),
            "event_type": event.get("event_type"),
            "content_id": event.get("content_id"),
            "watch_time_mins": event.get("watch_time_mins", 0),
            "device": event.get("device"),
            "is_valid": is_valid,
            "validation_note": reason
        }

        if is_valid:
            self.valid_count += 1
            # 1. Mask IP for privacy before relational persistence
            ip_clean = mask_ip(event.get("ip_address", "127.0.0.1"))

            # 2. Persist to relational DB
            try:
                insert_sql = """
                INSERT OR IGNORE INTO events (event_id, user_id, event_type, content_id, watch_time_mins, device, session_id, timestamp, ip_address)
                VALUES (:event_id, :user_id, :event_type, :content_id, :watch_time_mins, :device, :session_id, :timestamp, :ip_address)
                """
                # Postgres fallback to standard ON CONFLICT DO NOTHING
                if self.db.db_type == "PostgreSQL":
                    insert_sql = """
                    INSERT INTO events (event_id, user_id, event_type, content_id, watch_time_mins, device, session_id, timestamp, ip_address)
                    VALUES (:event_id, :user_id, :event_type, :content_id, :watch_time_mins, :device, :session_id, :timestamp, :ip_address)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                
                self.db.execute_raw(insert_sql, {
                    "event_id": event.get("event_id"),
                    "user_id": event.get("user_id"),
                    "event_type": event.get("event_type"),
                    "content_id": event.get("content_id"),
                    "watch_time_mins": int(event.get("watch_time_mins", 0)),
                    "device": event.get("device", "Unknown"),
                    "session_id": event.get("session_id", "sess_stream"),
                    "timestamp": event.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    "ip_address": ip_clean
                })
            except Exception as e:
                logger.warning("Relational streaming insert warning: %s", e)

            # 3. Persist raw doc to MongoDB / local document store
            self.mongo.insert_event({
                "event_id": event.get("event_id"),
                "user_id": event.get("user_id"),
                "event": event.get("event_type"),
                "content_id": event.get("content_id"),
                "timestamp": event.get("timestamp"),
                "metadata": {
                    "device": event.get("device"),
                    "watch_time_mins": event.get("watch_time_mins", 0),
                    "source": "kafka_stream_consumer"
                }
            })
        else:
            self.invalid_count += 1
            self.invalid_events_log.append(record_entry)
            if len(self.invalid_events_log) > 200:
                self.invalid_events_log.pop(0)

        self.consumed_events_log.append(record_entry)
        if len(self.consumed_events_log) > 500:
            self.consumed_events_log.pop(0)

        return record_entry

    def consume_from_local_buffer(self, max_items: int = 10) -> List[Dict[str, Any]]:
        """Processes available items from local streaming simulation buffer."""
        processed = []
        count = 0
        while LOCAL_STREAM_BUFFER and count < max_items:
            event = LOCAL_STREAM_BUFFER.pop(0)
            res = self.process_event(event)
            processed.append(res)
            count += 1
        return processed

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "total_consumed": self.consumed_count,
            "valid_events": self.valid_count,
            "invalid_events": self.invalid_count,
            "buffer_depth": len(LOCAL_STREAM_BUFFER),
            "valid_rate": round((self.valid_count / self.consumed_count * 100), 1) if self.consumed_count else 100.0
        }


consumer = EventConsumer()
