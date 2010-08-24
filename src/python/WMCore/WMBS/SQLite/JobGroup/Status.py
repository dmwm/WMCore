#!/usr/bin/env python
"""
_Status_
MySQL implementation of JobGroup.Status

"""

from WMCore.WMBS.MySQL.JobGroup.Status import Status as StatusJobGroupMySQL

class Status(StatusJobGroupMySQL):
    sql = StatusJobGroupMySQL.sql
        
    def execute(self, group=None, conn = None, transaction = False):
        StatusJobGroupMySQL.execute(self, group=group, conn=conn, transaction=transaction)
