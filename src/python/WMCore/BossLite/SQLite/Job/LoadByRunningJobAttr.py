#!/usr/bin/env python
"""
_LoadByRunningJobAttr_

SQLite implementation of BossLite.Jobs.LoadByRunningJobAttr
"""

__all__ = []
__revision__ = "$Id: LoadByRunningJobAttr.py,v 1.1 2010/04/28 21:14:40 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.LoadByRunningJobAttr import LoadByRunningJobAttr as MySQLLoadByRunningJobAttr

class LoadByRunningJobAttr(MySQLLoadByRunningJobAttr):
    """
    Same as MySQL

    """
