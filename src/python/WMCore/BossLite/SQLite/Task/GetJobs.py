#!/usr/bin/env python
"""
_GetJobs_

SQLite implementation of BossLite.Task.GetJobs
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.GetJobs import GetJobs as MySQLGetJobs

class GetJobs(MySQLGetJobs):
    """
    Identical to MySQL

    """
