"""
DataStream AI - MongoDB Integration Module
Stores raw/unstructured event documents with flexible metadata.
Provides zero-failure local JSON file fallback if MongoDB is not running.
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("datastream.mongodb")

BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_STORE_PATH = BASE_DIR / "data" / "raw_events_store.json"


class MongoManager:
    _instance: Optional["MongoManager"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoManager, cls).__new__(cls)
            cls._instance._client = None
            cls._instance._db = None
            cls._instance._is_connected = False
            cls._instance._init_connection()
        return cls._instance

    def _init_connection(self):
        mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        db_name = os.getenv("MONGODB_DATABASE", "datastream_events")

        try:
            from pymongo import MongoClient
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=1500)
            # Trigger server check
            client.admin.command("ping")
            self._client = client
            self._db = client[db_name]
            self._is_connected = True
            logger.info("Connected successfully to MongoDB database: %s", db_name)
        except Exception as e:
            self._is_connected = False
            logger.info("MongoDB unavailable (%s). Using local JSON event fallback at: %s", e, LOCAL_STORE_PATH)
            LOCAL_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
            if not LOCAL_STORE_PATH.exists():
                with open(LOCAL_STORE_PATH, "w", encoding="utf-8") as f:
                    json.dump([], f)

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    @property
    def storage_mode(self) -> str:
        return "MongoDB (Live Cluster)" if self._is_connected else "Local Document Store (JSON Fallback)"

    def insert_event(self, event_doc: Dict[str, Any]) -> bool:
        """Inserts an unstructured raw event document."""
        if "inserted_at" not in event_doc:
            event_doc["inserted_at"] = datetime.utcnow().isoformat()

        if self._is_connected and self._db is not None:
            try:
                self._db["raw_events"].insert_one(event_doc)
                return True
            except Exception as e:
                logger.warning("MongoDB write failed (%s), writing to local store.", e)

        # Fallback store
        try:
            records = self._read_local_store()
            records.append(event_doc)
            # Keep store manageable (last 5000 events)
            if len(records) > 5000:
                records = records[-5000:]
            self._write_local_store(records)
            return True
        except Exception as e:
            logger.error("Failed to write to local fallback store: %s", e)
            return False

    def insert_many_events(self, docs: List[Dict[str, Any]]) -> int:
        """Inserts batch of unstructured raw events."""
        if not docs:
            return 0
        inserted_count = 0
        for doc in docs:
            if self.insert_event(doc):
                inserted_count += 1
        return inserted_count

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves most recent raw documents."""
        if self._is_connected and self._db is not None:
            try:
                cursor = self._db["raw_events"].find({}, {"_id": 0}).sort("inserted_at", -1).limit(limit)
                return list(cursor)
            except Exception as e:
                logger.warning("MongoDB read failed: %s. Using local store.", e)

        records = self._read_local_store()
        return list(reversed(records[-limit:]))

    def count_events(self) -> int:
        """Returns total raw event documents stored."""
        if self._is_connected and self._db is not None:
            try:
                return self._db["raw_events"].count_documents({})
            except Exception:
                pass
        return len(self._read_local_store())

    def _read_local_store(self) -> List[Dict[str, Any]]:
        if not LOCAL_STORE_PATH.exists():
            return []
        try:
            with open(LOCAL_STORE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _write_local_store(self, records: List[Dict[str, Any]]):
        LOCAL_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOCAL_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, default=str)


mongo_manager = MongoManager()

def get_mongo():
    return mongo_manager
