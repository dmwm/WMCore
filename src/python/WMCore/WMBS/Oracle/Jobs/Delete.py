#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Jobs.Delete

"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.Delete import Delete as DeleteJobMySQL

class Delete(DeleteJobMySQL):
    sql = DeleteJobMySQL.sql
