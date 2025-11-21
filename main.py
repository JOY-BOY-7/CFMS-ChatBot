from fastapi import FastAPI, UploadFile, File, HTTPException, Response, Request, Cookie
from fastapi.responses import StreamingResponse  # <--- NEW
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import numpy as np
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
# NEW IMPORTS BELOW
from src.data_manager import create_session, get_session, cleanup_sessions, save_downloadable_result, get_downloadable_result
from src.llm_engine import call_gemini_json, build_prompt_cached, get_cache_key
from src.execution import safe_exec
from src.utils import extract_json_from_response


app = FastAPI(title="SAP OData ChatBot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- BACKGROUND TASK ---
def run_cleanup_scheduler():
    while True:
        time.sleep(600)
        try:
            cleanup_sessions(timeout_seconds=3600)
        except Exception as e:
            print(f"Error during cleanup: {e}")

@app.on_event("startup")
async def startup_event():
    t = threading.Thread(target=run_cleanup_scheduler, daemon=True)
    t.start()
    print("🕒 Session Cleanup Scheduler Started (TTL: 1 Hour)")

# --- MODELS ---
class ODataRequest(BaseModel):
    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
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

# --- ENDPOINTS ---

@app.post("/connect/odata")
def connect_odata(req: ODataRequest, response: Response):
    try:
        final_url = req.url or os.getenv("SAP_ODATA_URL")
        final_user = req.username or os.getenv("SAP_USERNAME")
        final_pass = req.password or os.getenv("SAP_PASSWORD")

        if not final_url or not final_user or not final_pass:
            raise ValueError("Missing OData Credentials.")

        df_raw = fetch_odata_cached(final_url, final_user, final_pass, req.timeout)
        key = f"odata::{final_url}"
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
            "message": "OData loaded successfully"
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

# --- NEW ENDPOINT: DOWNLOAD EXCEL ---
@app.get("/download/{download_id}")
def download_excel(download_id: str):
    """
    Generates an Excel file on the fly for the given download_id.
    """
    df = get_downloadable_result(download_id)
    
    if df is None:
        raise HTTPException(status_code=404, detail="Download link expired or invalid.")
        
    # Create Excel in Memory
    output = io.BytesIO()
    # Requires 'openpyxl' installed
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Result')
    
    output.seek(0)
    
    headers = {
        "Content-Disposition": f"attachment; filename=result_{download_id[:8]}.xlsx"
    }
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)


@app.post("/ask")
def ask_question(
    req: QueryRequest, 
    response: Response, 
    session_id_cookie: Optional[str] = Cookie(None, alias="session_id")
):
    final_session_id = req.session_id or session_id_cookie
    
    if not final_session_id:
        raise HTTPException(status_code=400, detail="Missing Session ID.")

    session = get_session(final_session_id)
    if not session:
        response.delete_cookie("session_id")
        raise HTTPException(status_code=404, detail="Session expired.")
    
    df = session["df"]
    schema_json = session["schema_json"]
    aliases = session["aliases"]
    
    prompt_preamble = build_prompt_cached(schema_json, aliases)
    full_prompt = prompt_preamble + "\nQuestion: " + req.question + "\nRespond ONLY with a JSON object containing keys: explain and expr."
    
    fingerprint = get_cache_key(schema_json, req.question)
    resp = call_gemini_json(req.gemini_url, req.gemini_key, full_prompt, schema_fingerprint=fingerprint)
    parsed = extract_json_from_response(resp)
    
    if not parsed or "expr" not in parsed:
        return {"answer": "Could not understand the question.", "generated_code": "", "result_table": [], "result_series": [], "download_id": None}
        
    expr = parsed["expr"]
    explanation = parsed.get("explain", "Executed successfully.")
    
    exec_result = safe_exec(expr, df)
    
    if exec_result["error"]:
        return {"answer": f"Error: {exec_result['error']}", "generated_code": expr, "result_table": [], "result_series": [], "download_id": None}
    
    result_obj = exec_result["result"]
    
    result_table = []
    result_series = []
    download_id = None  # <--- ID to hold the downloadable file

    is_scalar_df = False
    if isinstance(result_obj, pd.DataFrame):
        if result_obj.shape == (1, 1):
            result_obj = result_obj.iloc[0, 0]
            is_scalar_df = True

    if isinstance(result_obj, pd.DataFrame) and not is_scalar_df:
        df_clean = result_obj.where(pd.notnull(result_obj), None)
        result_table = df_clean.head(1000).to_dict(orient="records")
        
        # --- SAVE FOR DOWNLOAD ---
        # Only save if it's a DataFrame (tables are what people usually want to download)
        download_id = save_downloadable_result(result_obj)

    elif isinstance(result_obj, pd.Series):
        s_clean = result_obj.reset_index()
        s_clean = s_clean.where(pd.notnull(s_clean), None)
        result_series = s_clean.to_dict(orient="records")
        
        # --- SAVE FOR DOWNLOAD ---
        download_id = save_downloadable_result(s_clean)
        
    else:
        if result_obj is not None:
            val_str = str(result_obj)
            if isinstance(result_obj, (float, np.floating)):
                if 0 < abs(result_obj) < 0.01:
                    val_str = f"{result_obj:.8f}".rstrip("0").rstrip(".")
                else:
                    val_str = f"{result_obj:.2f}"
            explanation = explanation.strip().rstrip(".") + f" {val_str}"
        
    return {
        "answer": explanation,
        "generated_code": expr,
        "result_table": result_table,
        "result_series": result_series,
        "download_id": download_id  # <--- NEW FIELD
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
