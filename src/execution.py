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
    # The prompt rule says SQL must start with "sql:"
    if isinstance(expr, str) and expr.strip().lower().startswith("sql:"):
        if not HAS_DUCKDB:
             return {
                 "result": None, 
                 "error": "SQL requested but DuckDB is not installed/available.", 
                 "stdout": ""
             }
        
        try:
            # Remove the "sql:" prefix to get the raw query
            sql_query = expr.strip()[4:].strip()
            
            # Setup DuckDB in-memory
            con = duckdb.connect(database=':memory:')
            
            # Register the pandas DataFrame as a table named 'odata'
            # This allows the SQL "FROM odata" to work
            con.register('odata', df)
            
            # Execute
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
            # Try eval first (for expressions)
            try:
                result = eval(expr, {}, local_env)
            except Exception:
                # Fallback to exec (for statements)
                exec(expr, {}, local_env)
                result = None
                # Find the last object created (heuristic for 'return value')
                for k, v in reversed(list(local_env.items())):
                    if isinstance(v, (pd.DataFrame, pd.Series, plt.Figure)):
                        result = v
                        break
                if result is None:
                    result = "Code executed successfully (no return value)"
        except Exception:
            error = traceback.format_exc()
            
    return {"result": result, "error": error, "stdout": f.getvalue()}
