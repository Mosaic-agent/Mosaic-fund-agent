import httpx
import xml.etree.ElementTree as ET

url = "https://www.morningstar.in/handlers/autocompletehandler.ashx"
criteria = "DSP Multi Asset Omni Fund of Funds"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

with httpx.Client(timeout=15, follow_redirects=True) as client:
    resp = client.get(url, params={"criteria": criteria}, headers=headers)
    print(f"Status: {resp.status_code}")
    print(resp.text)
