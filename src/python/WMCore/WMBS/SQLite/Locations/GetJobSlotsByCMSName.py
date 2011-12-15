#!/usr/bin/env python
"""
_GetJobSlotsByCMSName_

SQLite implementation of Locations.GetJobSlotsByCMSName
"""

from WMCore.WMBS.MySQL.Locations.GetJobSlotsByCMSName \
    import GetJobSlotsByCMSName as MySQLGetJobSlotsByCMSName

class GetJobSlotsByCMSName(MySQLGetJobSlotsByCMSName):
    """
    Identical to MySQL version
    """