from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from pydantic import BaseModel
import pandas as pd
import io
import uvicorn
import json

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
    session_id: str
    question: str
    gemini_key: str
    gemini_url: str = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"

# --- Endpoints ---

@app.post("/connect/odata")
def connect_odata(req: ODataRequest):
    try:
        # Fetch
        df_raw = fetch_odata_cached(req.url, req.username, req.password, req.timeout)
        
        # Process
        key = f"odata::{req.url}"
        processed = build_processed_bundle_from_df(df_raw, key, use_duckdb=True)
        
        # Store Session
        session_id = create_session(processed)
        return {"session_id": session_id, "rows": len(processed["df"]), "message": "OData loaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/file")
async def upload_file(file: UploadFile = File(...)):
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
        return {"session_id": session_id, "rows": len(processed["df"]), "message": "File uploaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/ask")
def ask_question(req: QueryRequest):
    # 1. Retrieve Session
    session = get_session(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    
    df = session["df"]
    schema_json = session["schema_json"]
    aliases = session["aliases"]
    
    # 2. Build Prompt
    prompt_preamble = build_prompt_cached(schema_json, aliases)
    full_prompt = prompt_preamble + "\nQuestion: " + req.question + "\nRespond ONLY with a JSON object containing keys: explain and expr."
    
    # 3. Call Gemini
    resp = call_gemini_json(req.gemini_url, req.gemini_key, full_prompt)
    
    # 4. Extract Expression
    raw_msg = ""
    try:
        raw_msg = resp.get("candidates", [{}])[0].get("content", [{}])[0].get("parts", [{}])[0].get("text", "")
    except:
        raw_msg = str(resp)
        
    parsed = extract_json_from_response(raw_msg)
    
    if not parsed or "expr" not in parsed:
        return {
            "answer": "Could not understand the question.",
            "debug_raw": raw_msg
        }
        
    expr = parsed["expr"]
    explanation = parsed.get("explain", "Executed successfully.")
    
    # 5. Execute Code
    exec_result = safe_exec(expr, df)
    
    if exec_result["error"]:
        return {
            "answer": f"Error executing code: {exec_result['error']}",
            "generated_code": expr
        }
    
    result_obj = exec_result["result"]
    
    # 6. Format Output for JSON
    final_data = None
    result_type = "text"
    
    if isinstance(result_obj, pd.DataFrame):
        result_type = "table"
        # Limit rows for API performance
        final_data = result_obj.head(1000).to_dict(orient="records") 
    elif isinstance(result_obj, pd.Series):
        result_type = "table"
        final_data = result_obj.to_frame().to_dict(orient="records")
    else:
        final_data = str(result_obj)
        
    return {
        "answer": explanation,
        "generated_code": expr,
        "result_type": result_type,
        "data": final_data
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)