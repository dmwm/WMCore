#!/usr/bin/env python
"""
_Delete_

SQLite implementation of Masks.Delete
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.Delete import Delete as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql
