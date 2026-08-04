"""Forward to canonical location in src.data_importer."""
import runpy as _runpy
_runpy.run_module("src.data_importer.maintenance.db_backup", run_name="__main__", alter_sys=True)
