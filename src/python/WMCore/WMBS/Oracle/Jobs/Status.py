#!/usr/bin/env python
"""
_New_
Oracle implementation of Jobs.Status
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.Status import Status as StatusJobsMySQL

class Status(StatusJobsMySQL):
    sql = StatusJobsMySQL.sql
