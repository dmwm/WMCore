#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Masks.Delete
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.Delete import Delete as DeleteMasksMySQL

class Delete(DeleteMasksMySQL):
    sql = DeleteMasksMySQL.sql
