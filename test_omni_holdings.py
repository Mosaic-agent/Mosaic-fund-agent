import httpx
import json

_SAL_BASE = "https://api-global.morningstar.com/sal-service/v1"
_API_KEY = "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "apikey": _API_KEY,
}
_PARAMS = {
    "clientId": "MDC",
    "version": "4.71.0",
    "premiumNum": "10000",
    "freeNum": "10000",
}

sec_id = "F00001H69S"
url = f"{_SAL_BASE}/fund/portfolio/holding/v2/{sec_id}/data"

with httpx.Client(timeout=30, follow_redirects=True) as client:
    resp = client.get(url, headers=_HEADERS, params=_PARAMS)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(json.dumps(data, indent=2))
    else:
        print(resp.text)
