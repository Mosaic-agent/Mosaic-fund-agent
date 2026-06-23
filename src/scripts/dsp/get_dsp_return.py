
import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# Add src to path
sys.path.append(os.getcwd())

try:
    from config.settings import settings
except ImportError as e:
    print(f"Error: {e}")
    sys.exit(1)

def get_return():
    from src.db.pool import get_client
    client = get_client()
    
    # Scheme 152056 is DSP Multi Asset Allocation Fund - Direct Plan - Growth
    scheme_code = '152056'
    
    # Get latest NAV
    latest_query = f"""
    SELECT nav, nav_date 
    FROM market_data.mf_nav FINAL
    WHERE scheme_code = '{scheme_code}'
    ORDER BY nav_date DESC
    LIMIT 1
    """
    latest_res = client.query(latest_query)
    if not latest_res.result_rows:
        print(f"No data found for scheme {scheme_code}")
        return
    
    latest_nav, latest_date = latest_res.result_rows[0]
    
    # Get NAV from ~1 year ago
    target_date = latest_date - timedelta(days=365)
    
    # Find closest date <= target_date
    past_query = f"""
    SELECT nav, nav_date
    FROM market_data.mf_nav FINAL
    WHERE scheme_code = '{scheme_code}'
      AND nav_date <= '{target_date.strftime('%Y-%m-%d')}'
    ORDER BY nav_date DESC
    LIMIT 1
    """
    past_res = client.query(past_query)
    if not past_res.result_rows:
        # Try finding closest date > target_date if none before
        past_query = f"""
        SELECT nav, nav_date
        FROM market_data.mf_nav FINAL
        WHERE scheme_code = '{scheme_code}'
          AND nav_date > '{target_date.strftime('%Y-%m-%d')}'
        ORDER BY nav_date ASC
        LIMIT 1
        """
        past_res = client.query(past_query)
        
    if not past_res.result_rows:
        print(f"Could not find historical data for scheme {scheme_code} around {target_date}")
        return
        
    past_nav, past_date = past_res.result_rows[0]
    
    # Calculate return
    abs_return = (latest_nav - past_nav) / past_nav * 100
    
    print(f"Fund: DSP Multi Asset Allocation Fund (Direct - Growth)")
    print(f"Scheme Code: {scheme_code}")
    print(f"Latest NAV: {latest_nav:.4f} (as of {latest_date})")
    print(f"1-Year Ago NAV: {past_nav:.4f} (as of {past_date})")
    print(f"1-Year Return: {abs_return:.2f}%")

if __name__ == "__main__":
    get_return()
