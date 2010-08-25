#!/usr/bin/env python
"""
_New_

SQLite implementation of BossLite.RunningJob.New
"""

__all__ = []



from WMCore.BossLite.MySQL.RunningJob.New import New as MySQLNew

class New(MySQLNew):
    """
    Identical to MySQL

    """
