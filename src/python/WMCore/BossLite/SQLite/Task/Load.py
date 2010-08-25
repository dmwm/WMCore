#!/usr/bin/env python
"""
_Load_

SQLite implementation of BossLite.Task.Load
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.Load import Load as MySQLLoad

class Load(MySQLLoad):
    """
    Same as MySQL

    """
