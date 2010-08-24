#!/usr/bin/env python
"""
_New_

SQLite implementation of BossLite.Task.New
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.New import New as MySQLNew

class New(MySQLNew):
    """
    Identical to MySQL

    """
