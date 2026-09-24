"""
DataStream AI - Kafka Event Stream Producer
Publishes simulated streaming OTT user events to Kafka topic 'user-events'.
Includes a zero-dependency local simulation buffer if Kafka broker is offline.
"""

import os
import json
import time
import random
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("datastream.kafka.producer")

SAMPLE_USERS = [f"U{i:04d}" for i in range(1001, 1021)]
SAMPLE_CONTENT = ["MOV101", "MOV102", "SER201", "SER202", "MOV103", "MOV104", "DOC301", "SER203", "MOV105", "SER204"]
SAMPLE_DEVICES = ["Smart TV", "Mobile", "Web", "Tablet"]
SAMPLE_EVENT_TYPES = [
    "user_login", "content_view", "video_watch", "search",
    "add_to_watchlist", "subscription", "logout"
]

# Local queue simulation buffer for demo mode
LOCAL_STREAM_BUFFER = []


class EventProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "user-events")
        self._producer = None
        self._is_kafka_connected = False
        self._init_producer()

    def _init_producer(self):
        try:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=1500,
                max_block_ms=1500
            )
            self._is_kafka_connected = True
            logger.info("Connected to Kafka broker at %s", self.bootstrap_servers)
        except Exception as e:
            self._is_kafka_connected = False
            logger.info("Kafka broker unavailable (%s). Operating in Local In-Memory Streaming Mode.", e)

    @property
    def is_kafka_connected(self) -> bool:
        return self._is_kafka_connected

    @property
    def mode(self) -> str:
        return "Kafka Cluster (Live)" if self._is_kafka_connected else "Local Simulator (Demo Mode)"

    def generate_random_event(self) -> Dict[str, Any]:
        """Generates a realistic OTT user event payload."""
        event_type = random.choices(
            SAMPLE_EVENT_TYPES,
            weights=[0.15, 0.25, 0.35, 0.10, 0.08, 0.02, 0.05],
            k=1
        )[0]

        event_id = f"EVT_{int(time.time()*1000)}_{random.randint(100, 999)}"
        user_id = random.choice(SAMPLE_USERS)
        content_id = random.choice(SAMPLE_CONTENT) if event_type in ["content_view", "video_watch", "add_to_watchlist", "search"] else None
        watch_time = random.randint(5, 120) if event_type == "video_watch" else 0
        device = random.choice(SAMPLE_DEVICES)
        session_id = f"sess_{random.randint(100, 999)}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ip = f"{random.randint(10, 200)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

        return {
            "event_id": event_id,
            "user_id": user_id,
            "event_type": event_type,
            "content_id": content_id,
            "watch_time_mins": watch_time,
            "device": device,
            "session_id": session_id,
            "timestamp": timestamp,
            "ip_address": ip
        }

    def emit_event(self, event: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Publishes an event to Kafka or the local stream buffer."""
        payload = event or self.generate_random_event()

        if self._is_kafka_connected and self._producer:
            try:
                self._producer.send(self.topic, value=payload)
                self._producer.flush(timeout=1)
                return {"success": True, "event": payload, "target": "Kafka", "status": "PUBLISHED"}
            except Exception as e:
                logger.warning("Kafka publish error: %s. Routing to local stream buffer.", e)

        # Local simulation fallback
        LOCAL_STREAM_BUFFER.append(payload)
        if len(LOCAL_STREAM_BUFFER) > 500:
            LOCAL_STREAM_BUFFER.pop(0)

        return {"success": True, "event": payload, "target": "Local Stream Buffer", "status": "EMITTED_LOCAL"}

    def emit_batch(self, count: int = 5) -> int:
        """Emits multiple events in a batch."""
        success_count = 0
        for _ in range(count):
            res = self.emit_event()
            if res["success"]:
                success_count += 1
        return success_count


producer = EventProducer()
