"""MongoDB access layer for the FastAPI backend.

Used to serve AI agent outputs and pipeline metadata.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / "myfile.env")


def get_db():
    import pymongo
    client = pymongo.MongoClient(os.getenv("MONGO_URI"))
    return client[os.getenv("MONGO_DATABASE")]


def latest_doc(collection):
    db = get_db()
    try:
        doc = db[collection].find_one(
            {}, sort=[("created_at", -1)])
    finally:
        db.client.close()
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


def recent_docs(collection, limit=10):
    db = get_db()
    try:
        docs = list(
            db[collection].find({}).sort("created_at", -1).limit(limit))
    finally:
        db.client.close()
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs


def latest_quality_issues(limit=10):
    return recent_docs("data_quality_logs", limit)
