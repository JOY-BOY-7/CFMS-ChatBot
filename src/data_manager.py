import uuid
import time

# In-memory store
SESSION_STORE = {}

def create_session(processed_bundle):
    session_id = str(uuid.uuid4())
    SESSION_STORE[session_id] = {
        "df": processed_bundle["df"],
        "schema_json": processed_bundle["schema_json"],
        "aliases": processed_bundle["aliases"],
        "last_accessed": time.time() # timestamp
    }
    return session_id

def get_session(session_id):
    if session_id in SESSION_STORE:
        # Update timestamp whenever user asks a question (keep session alive)
        SESSION_STORE[session_id]["last_accessed"] = time.time()
        return SESSION_STORE[session_id]
    return None

def cleanup_sessions(timeout_seconds=3600):
    """
    Removes sessions inactive for > timeout_seconds.
    Default: 3600 seconds (1 Hour).
    """
    now = time.time()
    # Create a list of keys to delete (cannot delete while iterating)
    expired_ids = [
        sid for sid, data in SESSION_STORE.items() 
        if (now - data["last_accessed"]) > timeout_seconds
    ]
    
    count = 0
    for sid in expired_ids:
        del SESSION_STORE[sid]
        count += 1
        
    if count > 0:
        print(f"🧹 CLEANUP: Removed {count} expired sessions.")
