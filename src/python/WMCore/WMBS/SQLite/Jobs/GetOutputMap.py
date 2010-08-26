#!/usr/bin/env python
"""
_GetOutputMap_

SQLite implementation of Jobs.GetOutputMap
"""

__revision__ = "$Id: GetOutputMap.py,v 1.1 2009/10/14 16:47:19 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetOutputMap import GetOutputMap as MySQLGetOutputMap

class GetOutputMap(MySQLGetOutputMap):
    """
    Identical to MySQL version for now.
    """
    pass
