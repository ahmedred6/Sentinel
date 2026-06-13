import sys
import os

_EVAL_DIR = os.path.dirname(__file__)
_SDK_DIR = os.path.abspath(os.path.join(_EVAL_DIR, "..", "sentinel-sdk"))
for _p in (_EVAL_DIR, _SDK_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)
