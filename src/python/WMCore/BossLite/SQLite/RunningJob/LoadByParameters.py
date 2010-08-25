#!/usr/bin/env python
"""
_LoadByParameters_

SQLite implementation of BossLite.RunningJob.LoadByParameters
"""

__all__ = []
__revision__ = "$Id: LoadByParameters.py,v 1.1 2010/03/30 10:25:20 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.RunningJob.LoadByParameters import LoadByParameters as MySQLLoadByParameters

class LoadByParameters(MySQLLoadByParameters):
    """
    Identical to MySQL

    """
