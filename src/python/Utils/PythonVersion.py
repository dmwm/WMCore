"""
Easily get the version of the python interpreter at runtime
"""

from __future__ import division # Jenkins CI

import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

WMCORE_PICKLE_PROTOCOL = 2