import uuid
import time

# In-memory stores
SESSION_STORE = {}
DOWNLOAD_STORE = {}  # <--- NEW: Stores result DataFrames

# --- SESSION MANAGEMENT ---
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

# --- DOWNLOAD MANAGEMENT (NEW) ---
def save_downloadable_result(df):
    """Saves a dataframe temporarily and returns a unique ID."""
    download_id = str(uuid.uuid4())
    DOWNLOAD_STORE[download_id] = {
        "df": df,
        "created_at": time.time()
    }
    return download_id

def get_downloadable_result(download_id):
    """Retrieves the dataframe if it exists."""
    if download_id in DOWNLOAD_STORE:
        return DOWNLOAD_STORE[download_id]["df"]
    return None

# --- CLEANUP ---
def cleanup_sessions(timeout_seconds=3600):
    now = time.time()
    
    # 1. Clean Sessions
    expired_sessions = [sid for sid, data in SESSION_STORE.items() if (now - data["last_accessed"]) > timeout_seconds]
    for sid in expired_sessions:
        del SESSION_STORE[sid]
        
    # 2. Clean Downloads (NEW)
    # Downloads are temporary; delete them after 1 hour to free RAM
    expired_downloads = [did for did, data in DOWNLOAD_STORE.items() if (now - data["created_at"]) > timeout_seconds]
    for did in expired_downloads:
        del DOWNLOAD_STORE[did]

    if expired_sessions or expired_downloads:
        print(f"🧹 CLEANUP: Removed {len(expired_sessions)} sessions and {len(expired_downloads)} download links.")
