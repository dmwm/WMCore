#!/usr/bin/env python
"""
_SetCache_

SQLite implementation of Jobs.SetCache
"""

__all__ = []
__revision__ = "$Id: SetCache.py,v 1.1 2009/09/09 19:24:03 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.SetCache import SetCache as MySQLSetCache

class SetCache(MySQLSetCache):
    """
    Identical to MySQL version for now

    """
