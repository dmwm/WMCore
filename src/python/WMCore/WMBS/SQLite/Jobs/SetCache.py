#!/usr/bin/env python
"""
_SetCache_

SQLite implementation of Jobs.SetCache
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.SetCache import SetCache as MySQLSetCache

class SetCache(MySQLSetCache):
    """
    Identical to MySQL version for now

    """
