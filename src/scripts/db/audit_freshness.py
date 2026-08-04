"""Forward to canonical location in src.data_importer."""
import runpy as _runpy
_runpy.run_module("src.data_importer.maintenance.audit_freshness", run_name="__main__", alter_sys=True)
