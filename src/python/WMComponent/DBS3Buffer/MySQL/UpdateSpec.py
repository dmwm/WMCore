#!/usr/bin/env python
"""
_InsertSpec_

MySQL implementation of DBSBuffer3.UpdateSpec

Created on Mar 11, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateSpec(DBFormatter):
    """
    _UpdateSpec_

    Insert a spec using the name and task to a pre-existing record in dbsbuffer_workflow
    """

    existsSQL = "SELECT id FROM dbsbuffer_workflow WHERE name = :name AND task = :task"

    updateSQL = "UPDATE dbsbuffer_workflow SET spec = :spec WHERE id = :id"

    def execute(self, requestName, taskPath,
                specPath, conn = None, transaction = False):
        """
        _execute_

        Update a workflow in the dbsbuffer_workflow table
        """
        binds = {'name': requestName, 'task': taskPath}

        result = self.dbi.processData(self.existsSQL, binds, conn = conn,
                                      transaction = transaction)
        formattedResult = self.formatDict(result)

        if not formattedResult:
            return None

        workflowId = formattedResult[0]['id']

        if specPath is not None:
            binds = {'spec' : specPath, 'id' : workflowId}
            self.dbi.processData(self.updateSQL, binds, conn = conn,
                                 transaction = transaction)

        return workflowId
