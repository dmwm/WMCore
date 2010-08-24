#!/usr/bin/env python
"""
_Delete_

SQLite implementation of BossLite.Job.Delete
"""

__all__ = []



from WMCore.BossLite.MySQL.Job.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    Delete some files.

    """
