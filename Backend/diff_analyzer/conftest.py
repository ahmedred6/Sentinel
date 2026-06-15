import sys
import os

_DIFF_DIR = os.path.dirname(__file__)
_EVAL_DIR = os.path.abspath(os.path.join(_DIFF_DIR, "..", "eval_engine"))
_SDK_DIR = os.path.abspath(os.path.join(_DIFF_DIR, "..", "sentinel-sdk"))

for _p in (_SDK_DIR, _EVAL_DIR, _DIFF_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)
