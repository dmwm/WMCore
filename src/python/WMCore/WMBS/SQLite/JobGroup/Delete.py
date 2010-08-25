#!/usr/bin/env python
"""
_Delete_

SQLite implementation of JobGroup.Delete
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.Delete import Delete as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql
