import requests
import re
import ssl
import xml.etree.ElementTree as ET
import pandas as pd
import urllib3
from requests.adapters import HTTPAdapter
from functools import lru_cache  # <--- 1. NEW IMPORT

# Disable warnings globally for the module
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.options |= 0x4  # SSL_OP_LEGACY_SERVER_CONNECT
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = ctx
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

def parse_odata_xml(xml_text):
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
    }
    root = ET.fromstring(xml_text)
    entries = root.findall('.//atom:entry', ns)
    data = []
    for entry in entries:
        props = entry.find('.//m:properties', ns)
        if props is None:
            continue
        record = {}
        for child in props:
            tag = re.sub(r'^{.*}', '', child.tag)
            record[tag] = child.text
        data.append(record)
    if not data:
        raise ValueError("No valid data entries found in OData response.")
    return pd.DataFrame(data)

# 2. NEW DECORATOR: Caches the result of this function in RAM
@lru_cache(maxsize=32)
def fetch_odata_cached(odata_url: str, username: str = "", password: str = "", timeout: int = 30):
    """
    Fetch OData and return a DataFrame.
    Note: This function performs the raw fetch. Caching is handled by the session manager.
    """
    s = requests.Session()
    s.mount("https://", SSLAdapter())

    auth = (username, password) if username and password else None

    # Ensure $format=json preference
    if "$format" not in odata_url:
        sep = "&" if "?" in odata_url else "?"
        url_with_format = odata_url + f"{sep}$format=json"
    else:
        url_with_format = odata_url

    resp = s.get(url_with_format, auth=auth, headers={"Accept": "application/json,application/atom+xml"}, timeout=timeout, verify=False)

    if resp.status_code == 401:
        raise PermissionError("401 Unauthorized (wrong username/password)")
    if resp.status_code != 200:
        raise ConnectionError(f"OData fetch failed: {resp.status_code}: {resp.text[:1000]}")

    content_type = resp.headers.get("Content-Type", "").lower()
    if "json" in content_type:
        js = resp.json()
        if isinstance(js, dict) and "d" in js:
            results = js["d"].get("results", [])
        elif isinstance(js, dict) and "value" in js:
            results = js.get("value", [])
        else:
            results = js
        df = pd.DataFrame(results)
    else:
        df = parse_odata_xml(resp.text)

    return df
