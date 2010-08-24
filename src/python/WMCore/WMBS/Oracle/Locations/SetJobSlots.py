#!/usr/bin/env python
"""
_SetJobSlots_

Oracle implementation of Locations.SetJobSlots
"""

from WMCore.WMBS.MySQL.Locations.SetJobSlots import SetJobSlots as MySQLSetJobSlots

class SetJobSlots(MySQLSetJobSlots):
    """
    Identical to MySQL version
    """
