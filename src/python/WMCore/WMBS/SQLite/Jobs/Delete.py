#!/usr/bin/env python
"""
_Delete_

SQLite implementation of Jobs.Delete
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.Delete import Delete as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql
