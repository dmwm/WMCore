"""
Easily get the version of the python interpreter at runtime
"""

 # Jenkins CI

import sys
import pickle

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

# We need to keep compatibility between multiple python versions
# Further details at: https://github.com/dmwm/WMCore/pull/10726
# For PY2: set highest protocol to 2
# For PY3: set highest protocol to 4 (compatible with python3.6 and python3.8)
HIGHEST_PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL if PY2 else 4