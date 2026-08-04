"""Forward to canonical location in src.data_importer."""
import runpy as _runpy
_runpy.run_module("src.data_importer.maintenance.run_data_sanity_check", run_name="__main__", alter_sys=True)
