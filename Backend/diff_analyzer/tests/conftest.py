import sys
import os

_DIFF_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_EVAL_DIR = os.path.abspath(os.path.join(_DIFF_DIR, "..", "eval_engine"))
_SDK_DIR = os.path.abspath(os.path.join(_DIFF_DIR, "..", "sentinel-sdk"))

# Ensure diff_analyzer/ is at the front of sys.path so its modules take precedence
for _p in (_SDK_DIR, _EVAL_DIR, _DIFF_DIR):
    if _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

# Clear cached modules to avoid collision with same-named modules in other packages
for _mod in ("worker", "filter", "attribution", "experiments", "layers"):
    if _mod in sys.modules:
        del sys.modules[_mod]
