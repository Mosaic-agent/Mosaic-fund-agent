
import os
import sys
from datetime import datetime

# Add src to path
sys.path.append(os.getcwd())

from scripts.import_all_dsp_equity import run_import, ZIP_FILES

if __name__ == "__main__":
    # ZIP_FILES is a list of (date_str, url)
    # Get the latest one (index -1)
    latest_month = [ZIP_FILES[-1]]
    print(f"Importing latest month: {latest_month[0][0]}")
    run_import(months=latest_month, dry_run=False)
