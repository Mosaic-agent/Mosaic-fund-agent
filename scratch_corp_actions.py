import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.tools.market.equity import get_corporate_actions
from scratch_compare_anomalies import main as run_comparison

def main():
    print("Fetching and storing corporate actions for MSUMI...")
    try:
        res = get_corporate_actions.run({"symbol": "MSUMI"})
    except Exception:
        res = get_corporate_actions.func(symbol="MSUMI")
    print(res)
    
    print("\n" + "="*50)
    print("RE-RUNNING ANOMALY COMPARISON WITH CORPORATE ACTIONS IN DB:")
    print("="*50)
    run_comparison()

if __name__ == "__main__":
    main()
