from fastapi import FastAPI, UploadFile, File, HTTPException, Response, Request, Cookie
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import io
import uvicorn
import json
import os
import threading  
import time       
from typing import Optional, List, Dict, Any


# Import local modules
from src.data_loader import fetch_odata_cached
from src.data_processor import build_processed_bundle_from_df
from src.data_manager import create_session, get_session, cleanup_sessions # <--- Added cleanup_sessions
from src.llm_engine import call_gemini_json, build_prompt_cached, get_cache_key
from src.execution import safe_exec
from src.utils import extract_json_from_response


app = FastAPI(title="SAP OData ChatBot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows ALL domains (Use specific domains in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# --- BACKGROUND TASK: CLEANUP LOOP ---
def run_cleanup_scheduler():
    """
    Runs in the background. Checks every 10 minutes (600s).
    Deletes sessions older than 1 hour (3600s).
    """
    while True:
        time.sleep(600)  # Wait 10 minutes
        try:
            cleanup_sessions(timeout_seconds=3600)
        except Exception as e:
            print(f"Error during cleanup: {e}")

@app.on_event("startup")
async def startup_event():
    # Start the background thread as a Daemon (dies when main app dies)
    t = threading.Thread(target=run_cleanup_scheduler, daemon=True)
    t.start()
    print("🕒 Session Cleanup Scheduler Started (TTL: 1 Hour)")

# --- Pydantic Models & Helpers (Same as before) ---
class ODataRequest(BaseModel):
    url: Optional[str] = None       # Optional
    username: Optional[str] = None  # Optional
    password: Optional[str] = None  # Optional
    timeout: int = 30

class QueryRequest(BaseModel):
    session_id: Optional[str] = None 
    question: str
    gemini_key: str
    gemini_url: str = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"

def get_preview_data(df: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
    df_head = df.head(limit)
    df_clean = df_head.where(pd.notnull(df_head), None)
    return df_clean.to_dict(orient="records")

# --- Endpoints (Same as before) ---

@app.post("/connect/odata")
def connect_odata(req: ODataRequest, response: Response):
    try:
        # 2. Logic: User Input > Env Variable > Error
        final_url = req.url or os.getenv("SAP_ODATA_URL")
        final_user = req.username or os.getenv("SAP_USERNAME")
        final_pass = req.password or os.getenv("SAP_PASSWORD")

        # Validation: If we still don't have credentials, stop.
        if not final_url or not final_user or not final_pass:
            raise ValueError("Missing OData Configuration. Please provide in request or set SAP_ODATA_URL/USER/PASS on server.")

        # Fetch
        df_raw = fetch_odata_cached(final_url, final_user, final_pass, req.timeout)
        
        # Process
        key = f"odata::{final_url}"
        processed = build_processed_bundle_from_df(df_raw, key, use_duckdb=True)
        
        # Store Session
        session_id = create_session(processed)
        
        # Cookie Automation
        response.set_cookie(key="session_id", value=session_id, httponly=True)
        
        # Generate Preview
        preview_rows = get_preview_data(processed["df"], limit=10)
        columns = list(processed["df"].columns)

        return {
            "session_id": session_id,
            "rows": len(processed["df"]),
            "columns": columns,
            "sample_data": preview_rows,
            "message": "OData loaded successfully (Used Server Credentials)"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/file")
async def upload_file(response: Response, file: UploadFile = File(...)):
    try:
        contents = await file.read()
        if file.filename.endswith('.csv'):
            df_raw = pd.read_csv(io.BytesIO(contents))
        else:
            df_raw = pd.read_excel(io.BytesIO(contents))
            
        key = f"upload::{file.filename}"
        processed = build_processed_bundle_from_df(df_raw, key, use_duckdb=True)
        session_id = create_session(processed)
        
        response.set_cookie(key="session_id", value=session_id, httponly=True)
        
        preview_rows = get_preview_data(processed["df"], limit=10)
        columns = list(processed["df"].columns)
        
        return {
            "session_id": session_id,
            "rows": len(processed["df"]),
            "columns": columns,
            "sample_data": preview_rows,
            "message": "File uploaded successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/ask")
def ask_question(
    req: QueryRequest, 
    response: Response, 
    session_id_cookie: Optional[str] = Cookie(None, alias="session_id")
):
    final_session_id = req.session_id or session_id_cookie
    
    if not final_session_id:
        raise HTTPException(status_code=400, detail="Missing Session ID. Please upload a file first.")

    session = get_session(final_session_id)
    if not session:
        response.delete_cookie("session_id")
        raise HTTPException(status_code=404, detail="Session not found or expired (Timeout 1 Hour).")
    
    df = session["df"]
    schema_json = session["schema_json"]
    aliases = session["aliases"]
    
    prompt_preamble = build_prompt_cached(schema_json, aliases)
    full_prompt = prompt_preamble + "\nQuestion: " + req.question + "\nRespond ONLY with a JSON object containing keys: explain and expr."
    
    fingerprint = get_cache_key(schema_json, req.question)
    
    # 3. Call Gemini (Pass the fingerprint!)
    resp = call_gemini_json(req.gemini_url, req.gemini_key, full_prompt, schema_fingerprint=fingerprint)
    parsed = extract_json_from_response(resp)
    
    if not parsed or "expr" not in parsed:
        raw_text = str(resp)
        return {
            "answer": "Could not understand the question.",
            "generated_code": "",
            "result_table": [],
            "result_series": [],
            "debug_raw": raw_text
        }
        
    expr = parsed["expr"]
    explanation = parsed.get("explain", "Executed successfully.")
    
    exec_result = safe_exec(expr, df)
    
    if exec_result["error"]:
        return {
            "answer": f"Error executing code: {exec_result['error']}",
            "generated_code": expr,
            "result_table": [],
            "result_series": []
        }
    
    result_obj = exec_result["result"]
    
    result_table = []
    result_series = []

    if isinstance(result_obj, pd.DataFrame):
        df_clean = result_obj.where(pd.notnull(result_obj), None)
        result_table = df_clean.head(1000000).to_dict(orient="records")

    elif isinstance(result_obj, pd.Series):
        s_clean = result_obj.reset_index()
        s_clean = s_clean.where(pd.notnull(s_clean), None)
        result_series = s_clean.to_dict(orient="records")
        
    else:
        if result_obj is not None:
            explanation = explanation.strip()
            if explanation.endswith("."):
                explanation = explanation[:-1]
            explanation = f"{explanation} {result_obj}"
        
    return {
        "answer": explanation,
        "generated_code": expr,
        "result_table": result_table,
        "result_series": result_series
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
