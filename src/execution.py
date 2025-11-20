import io
import contextlib
import traceback
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from src.utils import fuzzy_filter

def safe_exec(expr, df):
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
                # Find the last object created
                for k, v in reversed(list(local_env.items())):
                    if isinstance(v, (pd.DataFrame, pd.Series, plt.Figure)):
                        result = v
                        break
                if result is None:
                    result = "Code executed successfully (no return value)"
        except Exception:
            error = traceback.format_exc()
            
    return {"result": result, "error": error, "stdout": f.getvalue()}