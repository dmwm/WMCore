#!/usr/bin/env python
"""
_Save_

SQLite implementation of BossLite.RunningJob.Save
"""

__all__ = []



from WMCore.BossLite.MySQL.RunningJob.Save import Save as MySQLSave

class Save(MySQLSave):
    """
    Identical to MySQL

    """
