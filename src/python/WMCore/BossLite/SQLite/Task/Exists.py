#!/usr/bin/env python
"""
_Exists_

SQLite implementation of BossLite.Task.Exists
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    Identical to MySQL

    """
