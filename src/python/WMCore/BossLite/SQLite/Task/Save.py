#!/usr/bin/env python
"""
_Save_

SQLite implementation of BossLite.Task.Save
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.Save import Save as MySQLSave

class Save(MySQLSave):
    """
    Identical to MySQL

    """
