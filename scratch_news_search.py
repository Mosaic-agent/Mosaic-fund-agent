import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.tools.market.equity import search_anomaly_events

def main():
    print("Running search_anomaly_events for MSUMI (365 days) via run()...")
    try:
        report = search_anomaly_events.run({"symbol": "MSUMI", "days": 365})
    except Exception as e:
        print(f"run() failed: {e}. Trying .func...")
        report = search_anomaly_events.func(symbol="MSUMI", days=365)
    print(report)

if __name__ == "__main__":
    main()
