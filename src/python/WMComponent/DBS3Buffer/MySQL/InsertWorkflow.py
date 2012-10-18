#!/usr/bin/env python
"""
_InsertWorkflow_

MySQL implementation of DBSBuffer.InsertWorkflow
"""

from WMCore.Database.DBFormatter import DBFormatter

class InsertWorkflow(DBFormatter):
    """
    _InsertWorkflow_

    Insert a workflow using the name and task
    """

    sql = "INSERT IGNORE INTO dbsbuffer_workflow (name, task) VALUES (:name, :task)"

    existsSQL = "SELECT id FROM dbsbuffer_workflow WHERE name = :name AND task = :task"

    updateSQL = "UPDATE dbsbuffer_workflow SET spec = :spec WHERE id = :id"


    def execute(self, requestName, taskPath, specPath = None, conn = None, transaction = False):
        """
        _execute_

        Insert a simple workflow into the dbsbuffer_workflow table
        """
        binds = {'name': requestName, 'task': taskPath}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData(self.existsSQL, binds, conn = conn,
                                      transaction = transaction)
        workflowId = self.formatDict(result)[0]['id']

        if specPath:
            # We got a specPath, update it in the database
            binds = {'spec' : specPath, 'id' : workflowId}
            self.dbi.processData(self.updateSQL, binds, conn = conn,
                                 transaction = transaction)

        return workflowId
