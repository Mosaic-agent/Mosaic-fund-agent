import sys
import os
import argparse
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from config.settings import settings
try:
    from src.data_importer.fetchers.shoonya_fetcher import _save_session
except ImportError:
    from src.importer.fetchers.shoonya_fetcher import _save_session

def main():
    parser = argparse.ArgumentParser(description="Authenticate Shoonya Broker Session")
    parser.add_argument("--code", help="The OAuth code copied from the redirect URL")
    args = parser.parse_args()

    user = getattr(settings, "shoonya_user_id", "")
    secret = getattr(settings, "shoonya_api_secret", "")
    
    if not user or not secret:
        print("Error: Shoonya credentials (SHOONYA_USER_ID, SHOONYA_API_SECRET) not set in .env")
        sys.exit(1)

    login_url = f"https://api.shoonya.com/OAuthlogin/investor-entry-level/login?api_key={user}&route_to={user}"

    if not args.code:
        print("=" * 60)
        print("SHOONYA OAUTH LOGIN FLOW")
        print("=" * 60)
        print("1. Open the following URL in your browser:")
        print(f"   {login_url}")
        print("\n2. Log in with your password, secure questions, and TOTP.")
        print("3. Authorize the application.")
        print("4. Copy the value of the 'code' parameter from the redirect URL's query string.")
        print("5. Run this script again with the copied code:")
        print("   python src/scripts/portfolio/shoonya_login.py --code <YOUR_CODE>")
        print("   (or via Docker: ./mosaic.sh src/scripts/portfolio/shoonya_login.py --code <YOUR_CODE>)")
        print("=" * 60)
        sys.exit(0)

    try:
        from NorenRestApiPy.NorenApi import NorenApi
        class ShoonyaApiPy(NorenApi):
            def __init__(self):
                NorenApi.__init__(
                    self,
                    host="https://api.shoonya.com/NorenWClientAPI",
                    websocket="wss://api.shoonya.com/NorenWSAPI/",
                )
    except ImportError:
        print("Error: NorenRestApiPy is not installed.")
        sys.exit(1)

    api = ShoonyaApiPy()
    print("Exchanging OAuth code for session tokens...")
    user_clean = user.replace("_U", "")
    try:
        result = api.getAccessToken(
            authcode=args.code,
            Secret_Code=secret,
            client_id=user,
            UID=user_clean
        )
        if result is not None:
            asc_tok, usrid, ref_tok, actid = result
            susertoken = getattr(api, '_NorenApi__susertoken', asc_tok)
            _save_session({
                "susertoken": susertoken,
                "access_token": asc_tok,
                "userid": usrid,
                "accountid": actid
            })
            print(f"Success! Authenticated successfully as {usrid}.")
            print("Session token has been cached and saved to ClickHouse.")
        else:
            print("Error: OAuth token generation failed. The code might be expired or invalid.")
            sys.exit(1)
    except Exception as exc:
        print(f"Error during OAuth token exchange: {exc}")
        sys.exit(1)

if __name__ == "__main__":
    main()
