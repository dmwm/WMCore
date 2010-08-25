#!/usr/bin/env python
"""
_Delete_

SQLite implementation of BossLite.Task.Delete
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    Delete some files.

    """
