#!/usr/bin/env python
"""
_BLGenericDAO_

SQLite implementation of BossLite.BLGenericDAO
"""

__all__ = []
__revision__ = "$Id: BLGenericDAO.py,v 1.1 2010/05/04 15:36:19 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.BLGenericDAO import BLGenericDAO as sqliteBLGenericDAO

class BLGenericDAO(sqliteBLGenericDAO):
    """
    Identical to MySQL
    """
