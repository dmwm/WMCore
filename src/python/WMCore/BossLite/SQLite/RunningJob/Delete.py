#!/usr/bin/env python
"""
_Delete_

SQLite implementation of BossLite.RunningJob.Delete
"""

__all__ = []



from WMCore.BossLite.MySQL.RunningJob.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    Delete some files.

    """
