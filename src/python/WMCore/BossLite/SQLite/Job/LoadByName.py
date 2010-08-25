#!/usr/bin/env python
"""
_LoadByName_

SQLite implementation of BossLite.Job.LoadByName
"""

__all__ = []
__revision__ = "$Id: LoadByName.py,v 1.1 2010/03/30 10:17:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.LoadByName import LoadByName as MySQLLoadByName

class LoadByName(MySQLLoadByName):
    """
    Identical to MySQL

    """
