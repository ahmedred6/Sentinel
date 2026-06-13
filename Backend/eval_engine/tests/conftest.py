import sys
import os

_EVAL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SDK_DIR = os.path.abspath(os.path.join(_EVAL_DIR, "..", "sentinel-sdk"))

# Ensure eval_engine/ is at the front of sys.path
for _p in (_SDK_DIR, _EVAL_DIR):
    if _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

# ch_writer also has a worker.py; clear any cached modules so eval_engine's
# versions are imported fresh when the test module is collected.
for _mod in ("worker", "layers"):
    if _mod in sys.modules:
        del sys.modules[_mod]
