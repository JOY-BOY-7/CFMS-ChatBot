from fastapi import FastAPI, UploadFile, File, HTTPException, Response, Request, Cookie
from pydantic import BaseModel
import pandas as pd
import io
import uvicorn
import json
from typing import Optional, List, Dict, Any

# Import local modules
from src.data_loader import fetch_odata_cached
from src.data_processor import build_processed_bundle_from_df
from src.data_manager import create_session, get_session
from src.llm_engine import call_gemini_json, build_prompt_cached
from src.execution import safe_exec
from src.utils import extract_json_from_response

app = FastAPI(title="SAP OData ChatBot API")

# --- Pydantic Models ---
class ODataRequest(BaseModel):
    url: str
    username: str = ""
    password: str = ""
    timeout: int = 30

class QueryRequest(BaseModel):
    session_id: Optional[str] = None 
    question: str
    gemini_key: str
    gemini_url: str = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"

# --- Helper for Preview ---
def get_preview_data(df: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
    """Returns top N rows as a list of dicts, handling NaNs for JSON"""
    df_head = df.head(limit)
    # Replace NaN with None (which becomes null in JSON)
    df_clean = df_head.where(pd.notnull(df_head), None)
    return df_clean.to_dict(orient="records")

# --- Endpoints ---

@app.post("/connect/odata")
def connect_odata(req: ODataRequest, response: Response):
    try:
        # Fetch
        df_raw = fetch_odata_cached(req.url, req.username, req.password, req.timeout)
        
        # Process
        key = f"odata::{req.url}"
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
            "columns": columns,          # <--- NEW: List of column names
            "sample_data": preview_rows, # <--- NEW: First 10 rows
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
            
        # Process
        key = f"upload::{file.filename}"
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
            "columns": columns,          # <--- NEW
            "sample_data": preview_rows, # <--- NEW
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
    # 1. RESOLVE SESSION ID
    final_session_id = req.session_id or session_id_cookie
    
    if not final_session_id:
        raise HTTPException(status_code=400, detail="Missing Session ID. Please upload a file first.")

    # Retrieve Session
    session = get_session(final_session_id)
    if not session:
        response.delete_cookie("session_id")
        raise HTTPException(status_code=404, detail="Session not found or expired.")
    
    df = session["df"]
    schema_json = session["schema_json"]
    aliases = session["aliases"]
    
    # 2. Build Prompt
    prompt_preamble = build_prompt_cached(schema_json, aliases)
    full_prompt = prompt_preamble + "\nQuestion: " + req.question + "\nRespond ONLY with a JSON object containing keys: explain and expr."
    
    # 3. Call Gemini
    resp = call_gemini_json(req.gemini_url, req.gemini_key, full_prompt)
    
    # 4. Extract Expression
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
    
    # 5. Execute Code
    exec_result = safe_exec(expr, df)
    
    if exec_result["error"]:
        return {
            "answer": f"Error executing code: {exec_result['error']}",
            "generated_code": expr,
            "result_table": [],
            "result_series": []
        }
    
    result_obj = exec_result["result"]
    
    # 6. Format Output
    result_table = []
    result_series = []

    if isinstance(result_obj, pd.DataFrame):
        df_clean = result_obj.where(pd.notnull(result_obj), None)
        result_table = df_clean.head(1000).to_dict(orient="records")

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