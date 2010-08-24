#!/usr/bin/env python
"""
_LoadByRunningJobAttr_

SQLite implementation of BossLite.Jobs.LoadByRunningJobAttr
"""

__all__ = []



from WMCore.BossLite.MySQL.Job.LoadByRunningJobAttr import LoadByRunningJobAttr as MySQLLoadByRunningJobAttr

class LoadByRunningJobAttr(MySQLLoadByRunningJobAttr):
    """
    Same as MySQL

    """
