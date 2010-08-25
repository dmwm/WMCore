#!/usr/bin/env python
"""
_LoadByJobID_

SQLite implementation of BossLite.Job.LoadByJobID
"""

__all__ = []
__revision__ = "$Id: LoadByJobID.py,v 1.1 2010/03/30 10:16:49 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.LoadByJobID import LoadByJobID as MySQLLoadByJobID

class LoadByJobID(MySQLLoadByJobID):
    """
    Identical to MySQL

    """
