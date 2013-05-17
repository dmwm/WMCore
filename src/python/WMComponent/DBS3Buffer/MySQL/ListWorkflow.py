#!/usr/bin/env python
"""
_ListWorkflow_

MySQL implementation of DBS3Buffer.ListWorkflow

Created on May 2, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListWorkflow(DBFormatter):
    """
    _ListWorkflow_

    Get a workflow id given its name and task path.
    """
    sql = """SELECT id FROM dbsbuffer_workflow
               WHERE name = :name AND task = :task"""


    def execute(self, name, task, conn = None,
                transaction = False):
        """
        _execute_

        Retrieve the workflow id
        """
        binds = {"name": name, "task" : task}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)[0]
