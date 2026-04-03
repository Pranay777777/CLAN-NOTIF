import importlib.util
from pathlib import Path


_ROOT_PIPELINE = Path(__file__).resolve().parent.parent / "pipeline.py"
_spec = importlib.util.spec_from_file_location("legacy_pipeline", _ROOT_PIPELINE)
_module = importlib.util.module_from_spec(_spec)
assert _spec is not None and _spec.loader is not None
_spec.loader.exec_module(_module)

run_pipeline = _module.run_pipeline


if __name__ == "__main__":
    # Delegate execution to legacy root entrypoint for backward compatibility.
    import runpy

    runpy.run_path(str(_ROOT_PIPELINE), run_name="__main__")
