#!/usr/bin/env python
"""
_Exists_

SQLite implementation of BossLite.Job.Exists
"""

__all__ = []



from WMCore.BossLite.MySQL.Job.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    Identical to MySQL

    """
