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


    def execute(self, requestName, taskPath, conn = None, transaction = False):
        """
        _execute_

        Insert a simple workflow into the dbsbuffer_workflow table
        """
        binds = {'name': requestName, 'task': taskPath}
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData(self.existsSQL, binds, conn = conn,
                                      transaction = transaction)

        id = self.formatDict(result)[0]['id']
        return id
