#!/usr/bin/env python
"""
UpdateWorkflowsToCompleted
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateWorkflowsToCompleted(DBFormatter):
    """
    _UpdateWorkflosToCompleted_

    Update dbsbuffer workflow to complete
    """

    sql = """UPDATE dbsbuffer_workflow
             SET completed = 1 WHERE name = :name
          """

    def execute(self, workflowNames, conn = None, transaction = False):
        """
        _execute_

        Run the query
        """
        binds = []
        for name in workflowNames:
            bind = {"name" : name}

        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)

        return
