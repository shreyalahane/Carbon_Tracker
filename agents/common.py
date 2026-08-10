"""Shared helpers for the AI agent layer.

Handles MySQL/MongoDB connections and LangChain LLM setup with a safe
fallback when no Anthropic API key is configured.
"""

import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "myfile.env")

MONGO_COLLECTIONS = {
    "advice": "advisor_advice",
    "plan": "weekly_plans",
    "report": "esg_reports",
}


def get_mysql_connection():
    import pymysql
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE'),
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8mb4'
    )


def get_mongo_db():
    import pymongo
    client = pymongo.MongoClient(os.getenv("MONGO_URI"))
    return client, client[os.getenv('MONGO_DATABASE')]


def _sanitize(value):
    """Convert datetime/date (and nested containers) to BSON-safe types."""
    from datetime import date, datetime
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


def save_to_mongodb(collection, document):
    try:
        client, db = get_mongo_db()
        document['created_at'] = datetime.now()
        db[collection].insert_one(_sanitize(document))
        client.close()
        return True
    except Exception as e:
        print(f"MongoDB save error: {e}")
        return False


def get_llm(temperature=0.3):
    """Return a ChatAnthropic instance, or None if no valid API key."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or "your_" in api_key.lower() or api_key == "sk-ant-placeholder":
        return None
    try:
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307"),
            temperature=temperature,
            anthropic_api_key=api_key
        )
    except Exception as e:
        print(f"LLM init error (using fallback): {e}")
        return None


def run_llm(llm, system_prompt, user_prompt):
    """Run a prompt through the LLM. Returns text or None on failure."""
    if llm is None:
        return None
    try:
        from langchain.prompts import ChatPromptTemplate
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", user_prompt),
        ])
        chain = prompt | llm
        result = chain.invoke({})
        if hasattr(result, "content"):
            return result.content.strip()
        return str(result).strip()
    except Exception as e:
        print(f"LLM call error (using fallback): {e}")
        return None
