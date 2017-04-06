#!/usr/bin/env python
"""
_CountFilesByStatus_

MySQL implementation of DBSBuffer.CountFilesByStatus

"""

from __future__ import division
from WMCore.Database.DBFormatter import DBFormatter


class CountFilesByStatus(DBFormatter):
    """
    CountFilesByStatus

    Count the number of files in dbs based on their status

    """
    sql = """SELECT COUNT(*)
             FROM dbsbuffer_file
             WHERE status = :status"""

    def execute(self, status, conn=None, transaction=False):
        binds = {'status': status}

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        result = self.format(result)
        return result[0][0]
