#!/usr/bin/env python
"""
_GetOutputMap_

SQLite implementation of Jobs.GetOutputMap
"""




from WMCore.WMBS.MySQL.Jobs.GetOutputMap import GetOutputMap as MySQLGetOutputMap

class GetOutputMap(MySQLGetOutputMap):
    """
    Identical to MySQL version for now.
    """
    pass
