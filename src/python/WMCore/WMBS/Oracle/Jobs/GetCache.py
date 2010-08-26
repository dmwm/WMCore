#!/usr/bin/env python
"""
_GetCache_

Oracle implementation of Jobs.GetCache
"""

__all__ = []
__revision__ = "$Id: GetCache.py,v 1.1 2009/09/09 19:13:53 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetCache import GetCache as MySQLGetCache

class GetCache(MySQLGetCache):
    """
    Identical to MySQL version for now

    """
