#!/usr/bin/env python
"""
_SetCache_

SQLite implementation of Jobs.SetCache
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.GetCache import GetCache as MySQLGetCache

class GetCache(MySQLGetCache):
    """
    Identical to MySQL version for now

    """
