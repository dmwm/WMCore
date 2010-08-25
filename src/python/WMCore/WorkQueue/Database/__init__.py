#!/usr/bin/env python
"""
_Database_

Implementations for the various database backends.

"""
__all__ = []
__revision__ = "$Id: __init__.py,v 1.3 2009/11/12 16:43:31 swakef Exp $"
__version__ = "$Revision: 1.3 $"

States = {'Available' : 1,
          'Negotiating' : 2,
          'Acquired' : 3,
          'Done' : 4,
          'Failed' : 5,
          'Canceled' : 6}

# fill with index mapping for reverse lookup
for x, y in States.items():
    States[y] = x
