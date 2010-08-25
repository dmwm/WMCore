#!/usr/bin/env python
"""
_New_

SQLite implementation of BossLite.Job.New
"""

__all__ = []



from WMCore.BossLite.MySQL.Job.New import New as MySQLNew

class New(MySQLNew):
    """
    Identical to MySQL

    """
