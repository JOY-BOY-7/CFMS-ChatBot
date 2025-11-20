import pandas as pd
import time
import json
from src.utils import normalize_col, fuzzy_column_map

# Try importing duckdb
try:
    import duckdb
    HAS_DUCKDB = True
except Exception:
    HAS_DUCKDB = False

def build_processed_bundle_from_df(df_raw: pd.DataFrame, data_key: str = "", use_duckdb: bool = True):
    """
    Returns processed bundle dict: df, schema_json, aliases, fuzzy_map, etc.
    Note: Caching is now handled by the session manager, not this function.
    """
    t0 = time.time()
    orig_cols = df_raw.columns.tolist()
    norm_map = {c: normalize_col(c) for c in orig_cols}
    df_proc = df_raw.copy()
    df_proc.columns = [norm_map[c] for c in orig_cols]
    fuzzy_map = fuzzy_column_map(df_proc.columns)

    # coerce numeric columns where possible (safe)
    for c in df_proc.columns:
        df_proc[c] = pd.to_numeric(df_proc[c], errors='ignore')

    # build compact schema JSON
    schema = []
    for c in df_proc.columns:
        sample = ""
        try:
            s = df_proc[c].dropna()
            if s.shape[0] > 0:
                sample = str(s.iloc[0])[:25]  # short sample only
        except Exception:
            sample = ""
        schema.append({"name": c, "dtype": str(df_proc[c].dtype), "sample": sample})
    schema_json = json.dumps(schema, separators=(",", ":"), ensure_ascii=False)
    aliases = ", ".join(list(fuzzy_map.keys()))

    duckdb_registered = False
    if use_duckdb and HAS_DUCKDB:
        try:
            # Test registration to ensure data is compatible with DuckDB
            con = duckdb.connect(database=':memory:')
            con.register('odata_temp', df_proc)
            con.close()
            duckdb_registered = True
        except Exception:
            duckdb_registered = False

    elapsed = int((time.time() - t0) * 1000)
    return {
        "df": df_proc,
        "schema_json": schema_json,
        "aliases": aliases,
        "fuzzy_map": fuzzy_map,
        "duckdb_registered": duckdb_registered,
        "build_ms": elapsed
    }