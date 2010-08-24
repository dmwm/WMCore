#!/usr/bin/env python
"""
_Exists_

SQLite implementation of BossLite.RunningJob.Exists
"""

__all__ = []



from WMCore.BossLite.MySQL.RunningJob.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    Identical to MySQL

    """
