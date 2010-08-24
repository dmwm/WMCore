#!/usr/bin/env python
"""
_Load_

SQLite implementation of BossLite.Job.Load
"""

__all__ = []



from WMCore.BossLite.MySQL.Job.Load import Load as MySQLLoad

class Load(MySQLLoad):
    """
    Same as MySQL

    """
