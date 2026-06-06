import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.tools.report_publisher import publish_research_pdf

def main():
    print("Testing publish_research_pdf for MSUMI...")
    
    # Simple report markdown body
    markdown = """# Motherson Sumi Wiring India Limited (MSUMI)
This is a test research report generated to verify the integration of the anomaly regime clusters chart in the PDF output.

## Anomaly Analysis
We have run our quantitative GARCH(1,1) + Isolation Forest + PELT composite pipeline to detect anomalous price shifts and volatility regimes. Under the tighter sensitivity calibration ($z=3.0$, $contamination=0.03$), we flagged 9 major anomaly days over the past year.
"""
    
    try:
        res = publish_research_pdf.run({"symbol": "MSUMI", "report_markdown": markdown})
        print(res)
    except Exception as e:
        print(f"Failed with run(): {e}. Trying direct call...")
        res = publish_research_pdf.func(symbol="MSUMI", report_markdown=markdown)
        print(res)

if __name__ == "__main__":
    main()
