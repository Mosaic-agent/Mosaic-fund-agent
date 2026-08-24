"""Forward to canonical location in src.data_importer."""
import sys
from pathlib import Path
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import runpy as _runpy
_runpy.run_module("src.data_importer.maintenance.run_data_sanity_check", run_name="__main__", alter_sys=True)
