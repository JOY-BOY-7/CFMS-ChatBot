import requests
import json
import hashlib
from functools import lru_cache

# 1. Create a Global Session (Reuses TCP connection for speed)
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)

# 2. In-Memory Semantic Cache
# Key: Hash(Schema + Question) -> Value: JSON Response from Gemini
CODE_CACHE = {}

def get_cache_key(schema_json, question):
    """Creates a unique fingerprint for this specific question on this specific data."""
    raw = f"{schema_json}::{question.strip().lower()}"
    return hashlib.md5(raw.encode()).hexdigest()

def call_gemini_json(url, key, prompt, timeout=40, schema_fingerprint=None):
    """
    Updated to use Persistent Session AND Caching.
    """
    # CHECK CACHE FIRST (Speed: 0.001s)
    if schema_fingerprint and schema_fingerprint in CODE_CACHE:
        print("⚡ CACHE HIT: Skipping Gemini, using saved code.")
        return CODE_CACHE[schema_fingerprint]

    headers = {"x-goog-api-key": key, "Content-Type": "application/json"}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    try:
        # USE SESSION (Speed: Saves ~0.2s handshake)
        r = session.post(url, headers=headers, json=payload, timeout=timeout)
        resp_json = r.json()
        
        # SAVE TO CACHE if valid
        if schema_fingerprint and "candidates" in resp_json:
            CODE_CACHE[schema_fingerprint] = resp_json
            
        return resp_json
    except Exception as e:
        return {"error": str(e)}

@lru_cache(maxsize=20)
def build_prompt_cached(schema_json: str, aliases: str):
    PROMPT_PREAMBLE = f"""
You are a data analysis agent. The dataframe is available as a pandas DataFrame named `df`.
Schema (columns and types): {schema_json}
Column aliases (comma separated): {aliases}

Rules:
1. Use closest matching column names
2. String comparisons are case-insensitive and fuzzy (handled automatically)
3. Numeric operations safe
4. Never hallucinate columns/values
5. No loops/imports/prints
6. Never use print() or display()
7. Always RETURN the final result (DataFrame, Series, numeric, dict, or plt.Figure)
8. Always handle NaN values safely:
   - For string filters: use str.contains(..., na=False)
   - For numeric operations: safely handle empty sequences
9. Always assume the expression will be executed inside a safe environment that automatically displays the result.
10. Do not print anything — simply return the result or expression output.
11. Prefer one-liners that evaluate to a result directly (no variables unless necessary).
12. When multiple values are logically related (like total count + list), return a dictionary.
13. If visualization is the best answer, generate a matplotlib figure object (plt.figure()) and plot accordingly.
14. When grouping numeric columns, use aggregation (sum, mean, count).
15. Do not answer general knowledge questions (outside dataset); reply with "only ask questions related to data please".
16. SELF-CHECK COLUMN VALIDATION RULE:

When choosing the column to filter for a user value (e.g., "Professional Tax"),
perform the following steps:

1. Select the best matching string column using fuzzy matching.
2. Apply the filter on that column.
3. If the filter returns ZERO matching rows, do NOT return this result.
4. Instead, automatically try the NEXT BEST matching string column.
5. Continue checking all string columns in order of similarity.
6. Stop when you find a column that produces at least one matching row.
7. If no column produces any result, return:
     "No matching data found in the dataset."
8. Never assume that the first matched column is correct; always apply
   this self-check loop before finalizing the expression.
17.If the question logically implies a value search across multiple columns
(e.g., 'Professional Tax', 'GST', 'Jackson'),
never restrict search to a single guessed column.
Always use full multi-column search.
18.Automatically treat the search term as a generic value search.
Search it inside all string columns using OR across all columns.
Return all matching rows.
19.When working with dates, always convert using:
pd.to_datetime(df[column], dayfirst=True, errors='coerce')

Never assume the exact date format.
Always use dayfirst=True for dd-mm-yyyy formats.
Always handle invalid dates using errors='coerce'.
20.Avoid df.apply(..., axis=1) unless absolutely required.
Prefer vectorized operations because they are more reliable and efficient.
Only use axis=1 for complex row-level logic.
Always handle NaN safely in such cases.
21. STRING MATCHING (STRICT RULE):
When filtering strings, NEVER use the '==' operator.
ALWAYS use .str.contains('value', case=False, na=False).
The user rarely provides the exact full string, so partial matching is required.

22. ACRONYM & ABBREVIATION INTELLIGENCE (CRITICAL):
Users frequently use short codes (acronyms) for column names. You must map these to the correct column and NOT filter by the acronym itself.

Logic:
- If a word matches the initials of a column (e.g., 'sdh' -> 'sub_detailed_head', 'dh' -> 'detailed_head'), treats it as the COLUMN selector, not a data value.

Examples of Correct Logic:
A) User: "dh 001 amount"
   Analysis: "dh" = detailed_head (Column), "001" = Value to filter, "amount" = Column to show.
   Correct Expr: df[df['detailed_head'].str.contains('001', na=False)][['detailed_head', 'gross_amt']]

B) User: "sdh total amount"
   Analysis: "sdh" = sub_detailed_head (Column), "total amount" = Sum aggregation.
   Correct Expr: df.groupby('sub_detailed_head')['gross_amt'].sum()
   
   WRONG Expr (DO NOT DO THIS): df[df['sub_detailed_head'].str.contains('sdh', na=False)]...
   (Reason: 'sdh' is the column name alias, it is NOT a value inside the column rows.)
23.SQL MODE (MANDATORY):
- If the user message begins with “sql:”, then you MUST return SQL and NOTHING else.
- NEVER convert the SQL request to pandas or python.
- NEVER rewrite SQL into python.
- ALWAYS return an expression starting with “sql:” exactly like this (lowercase):
      sql: SELECT ... FROM odata ...
- ALWAYS query the table named `odata`.
- Write SQL in ONE line only.
- Valid example:
      sql: SELECT COUNT(*) FROM odata
- If the user writes “sql:”, pandas expressions like df.shape[0], df[...] are NOT allowed.
24.NATURAL LANGUAGE ANSWERING (CRITICAL):
- The 'explain' field MUST be the direct answer prefix. 
- BAD: "The user is asking for the total amount..."
- GOOD: "The total amount for Sub Detailed Head 012 is"
- NEVER mention column names (like 'sub_detailed_head') in the explanation. Use human-readable names (e.g., "Sub Detailed Head").
- Do NOT put the result number in the explanation (it will be calculated by the code).
25. FORMATTING NUMBERS:
- If the result is a float, round it to 2 decimal places in the explanation.
- Avoid scientific notation (e.g., 3.6e-05) in the explanation text. Convert to regular decimal or percentage.
"""
    return PROMPT_PREAMBLE
