#!/usr/bin/env python
"""
_GetCompletedWorkflows_

"""

from WMCore.Database.DBFormatter import DBFormatter

class GetCompletedWorkflows(DBFormatter):
    """
    _GetCompletedWorkflows_
    """
    sql = """SELECT name, count(name), sum(completed)
                FROM dbsbuffer_workflow
                GROUP BY name"""

    def execute(self, conn=None, transaction=False):
        """
        """
        result = self.dbi.processData(self.sql, conn = conn,
                                          transaction = transaction)
        workflowList = []
        for row in self.format(result):
            if row[1] == row[2]:
                workflowList.append(row[0])
        return workflowList
