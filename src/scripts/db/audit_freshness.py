"""Forward to canonical location in src.data_importer."""
import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

import runpy as _runpy
_runpy.run_module("src.data_importer.maintenance.audit_freshness", run_name="__main__", alter_sys=True)
