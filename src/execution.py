import io
import contextlib
import traceback
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from src.utils import fuzzy_filter

# Try importing duckdb
try:
    import duckdb
    HAS_DUCKDB = True
except Exception:
    HAS_DUCKDB = False

def safe_exec(expr, df):
    """
    Executes either Python Code or SQL Queries based on the prefix.
    """
    
    # --- 1. CHECK FOR SQL MODE ---
    if isinstance(expr, str) and expr.strip().lower().startswith("sql:"):
        if not HAS_DUCKDB:
             return {
                 "result": None, 
                 "error": "SQL requested but DuckDB is not installed/available.", 
                 "stdout": ""
             }
        
        try:
            sql_query = expr.strip()[4:].strip()
            con = duckdb.connect(database=':memory:')
            con.register('odata', df)
            result_df = con.execute(sql_query).df()
            con.close()
            return {
                "result": result_df, 
                "error": None, 
                "stdout": f"Executed SQL: {sql_query}"
            }
        except Exception as e:
             return {
                 "result": None, 
                 "error": f"SQL Execution Error: {str(e)}", 
                 "stdout": ""
             }

    # --- 2. PYTHON MODE (Fallback) ---
    local_env = {
        "df": df, 
        "pd": pd, 
        "np": np, 
        "plt": plt, 
        "re": re, 
        "fuzzy_filter": fuzzy_filter
    }

    f = io.StringIO()
    result = None
    error = None

    with contextlib.redirect_stdout(f):
        try:
            # Try eval first (for expressions like "df.head()")
            try:
                result = eval(expr, {}, local_env)
            except Exception:
                # Fallback to exec (for statements like "x = ...")
                exec(expr, {}, local_env)
                result = None
                # Find the last object created
                for k, v in reversed(list(local_env.items())):
                    if isinstance(v, (pd.DataFrame, pd.Series, plt.Figure)):
                        result = v
                        break
                if result is None:
                    result = "Code executed successfully (no return value)"
                    
        except SyntaxError:
            # --- 3. HANDLER FOR CONVERSATIONAL TEXT ---
            # If the AI returns plain text (e.g., "Hello" or "only ask questions..."),
            # Python raises a SyntaxError. We catch it and return the text as the result.
            result = str(expr)
            error = None
            
        except Exception:
            # For actual code errors (e.g., NameError, ValueError)
            error = traceback.format_exc()
            
    return {"result": result, "error": error, "stdout": f.getvalue()}
