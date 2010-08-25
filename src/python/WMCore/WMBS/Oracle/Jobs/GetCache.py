#!/usr/bin/env python
"""
_GetCache_

Oracle implementation of Jobs.GetCache
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.GetCache import GetCache as MySQLGetCache

class GetCache(MySQLGetCache):
    """
    Identical to MySQL version for now

    """
