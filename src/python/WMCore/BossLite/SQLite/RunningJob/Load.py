#!/usr/bin/env python
"""
_Load_

SQLite implementation of BossLite.RunningJob.Load
"""

__all__ = []



from WMCore.BossLite.MySQL.RunningJob.Load import Load as MySQLLoad

class Load(MySQLLoad):
    """
    Identical to MySQL

    """
