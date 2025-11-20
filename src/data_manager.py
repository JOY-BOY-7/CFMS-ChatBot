import uuid
import time

# In-memory store: { session_id: { "df": dataframe, "schema": json, "aliases": str, "timestamp": time } }
SESSION_STORE = {}

def create_session(processed_bundle):
    session_id = str(uuid.uuid4())
    SESSION_STORE[session_id] = {
        "df": processed_bundle["df"],
        "schema_json": processed_bundle["schema_json"],
        "aliases": processed_bundle["aliases"],
        "last_accessed": time.time()
    }
    return session_id

def get_session(session_id):
    if session_id in SESSION_STORE:
        SESSION_STORE[session_id]["last_accessed"] = time.time()
        return SESSION_STORE[session_id]
    return None

def cleanup_sessions(timeout_seconds=3600):
    """Removes old sessions to free memory"""
    now = time.time()
    to_remove = [k for k, v in SESSION_STORE.items() if now - v["last_accessed"] > timeout_seconds]
    for k in to_remove:
        del SESSION_STORE[k]