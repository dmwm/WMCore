#!/usr/bin/env python
"""
_Delete_

Oracle implementation of JobGroup.Delete

"""
__all__ = []



from WMCore.WMBS.MySQL.JobGroup.Delete import Delete as DeleteJobGroupMySQL

class Delete(DeleteJobGroupMySQL):
    sql = DeleteJobGroupMySQL.sql
