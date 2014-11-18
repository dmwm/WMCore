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
             SET completed = 1
             WHERE name = :WORKFLOW
             """

    def execute(self, workflows, conn = None, transaction = False):
        """
        _execute_

        Run the query
        """
        binds = []
        for workflow in workflows:
            binds.append( { "WORKFLOW" : workflow } )

        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)

        return
