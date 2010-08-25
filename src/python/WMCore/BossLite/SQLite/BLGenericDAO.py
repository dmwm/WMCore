#!/usr/bin/env python
"""
_BLGenericDAO_

SQLite implementation of BossLite.BLGenericDAO
"""

__all__ = []



from WMCore.BossLite.MySQL.BLGenericDAO import BLGenericDAO as sqliteBLGenericDAO

class BLGenericDAO(sqliteBLGenericDAO):
    """
    Identical to MySQL
    """
