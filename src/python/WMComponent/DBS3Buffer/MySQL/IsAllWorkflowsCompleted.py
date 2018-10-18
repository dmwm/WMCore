#!/usr/bin/env python
"""
_IsAllWorkflowsCompleted_

MySQL implementation of DBSBuffer.IsAllWorkflowsCompleted

"""

from __future__ import print_function, division
from WMCore.Database.DBFormatter import DBFormatter


class IsAllWorkflowsCompleted(DBFormatter):
    """
    _IsAllWorkflowsCompleted_

    check whether all workflows are completed
    """
    sql = """SELECT count(*) as count FROM dbsbuffer_workflow WHERE completed=0"""

    def execute(self, conn=None, transaction=False):
        """
        returns if there is no commpleted == 0 tasks which means all the workflows are in completed status
        """
        result = self.dbi.processData(self.sql, conn=conn,
                                      transaction=transaction)

        flag = True if self.format(result)[0][0] == 0 else False
        return flag
