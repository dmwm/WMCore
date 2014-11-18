#!/usr/bin/env python
"""
_GetCompletedWorkflows_

"""

from WMCore.Database.DBFormatter import DBFormatter

class GetCompletedWorkflows(DBFormatter):
    """
    _GetCompletedWorkflows_
    """
    sql = """SELECT distinct(name) FROM dbsbuffer_workflow 
               WHERE completed = 1"""

    def execute(self, conn = None, transaction = False):
        """
        """
        result = self.dbi.processData(self.sql, conn = conn,
                                          transaction = transaction)
        workflowList = []
        for row in self.format(result):
            workflowList.append(row[0])
        return workflowList
