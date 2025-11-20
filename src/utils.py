import re
import json
import difflib
import ast
import traceback
import io
import contextlib

# -----------------------------
# NORMALIZATION + FUZZY MATCHING
# -----------------------------

def normalize_col(c):
    return re.sub(r"[^0-9a-z_]", "_", c.strip().lower())

def fuzzy_column_map(columns):
    mapping = {}
    for c in columns:
        mapping[c.lower()] = c
        for token in c.lower().split("_"):
            mapping[token] = c
    return mapping

def fuzzy_filter(df, col, value):
    col_values = df[col].dropna().astype(str).unique()
    closest = difflib.get_close_matches(str(value), col_values, n=1, cutoff=0.6)
    if closest:
        return df[df[col].fillna('').str.contains(closest[0], case=False, na=False)]
    else:
        return df[df[col].fillna('').str.contains(str(value), case=False, na=False)]

# -----------------------------
# JSON EXTRACTION
# -----------------------------

def extract_json_from_response(resp):
    # Fix for python-stringified dicts
    if isinstance(resp, str) and resp.strip().startswith("{'") and resp.strip().endswith("}"):
        try:
            resp = ast.literal_eval(resp)
        except:
            pass

    text = ""

    try:
        # ---- Extract Gemini text safely ----
        if isinstance(resp, dict):
            c = resp.get("candidates", [{}])[0].get("content", {})

            # Format A: list
            if isinstance(c, list):
                parts = c[0].get("parts", [])
                if parts:
                    text = parts[0].get("text", "")
                else:
                    text = c[0].get("text", "")

            # Format B: dict
            elif isinstance(c, dict):
                parts = c.get("parts", [])
                if isinstance(parts, list) and parts:
                    text = parts[0].get("text", "")
                else:
                    text = c.get("text", "")

        else:
            text = str(resp)

        # Remove code fences
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text.strip())
        text = re.sub(r"\s*```$", "", text.strip())

        # Pull JSON block
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return None

        json_str = m.group(0).replace(r"\'", "'")

        # JSON first
        try:
            return json.loads(json_str)
        except:
            # fallback to python dict
            return ast.literal_eval(json_str)

    except:
        return None

# -----------------------------
# SAFETY CHECK
# -----------------------------

def validate_expr(expr):
    forbidden = [
        "subprocess", "os.", "sys.", "open(",
        "eval(", "exec(", "__import__", "input(", "print("
    ]
    if any(f in expr for f in forbidden):
        raise ValueError("Unsafe code detected.")
    return True
