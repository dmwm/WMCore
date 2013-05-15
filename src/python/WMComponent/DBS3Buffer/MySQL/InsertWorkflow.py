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

    sql = """INSERT IGNORE INTO dbsbuffer_workflow (name, task,
                                                    block_close_max_wait_time,
                                                    block_close_max_files,
                                                    block_close_max_events,
                                                    block_close_max_size)
                VALUES (:name, :task,
                        :blockMaxCloseTime,
                        :blockMaxFiles,
                        :blockMaxEvents,
                        :blockMaxSize)"""

    existsSQL = "SELECT id FROM dbsbuffer_workflow WHERE name = :name AND task = :task"


    def execute(self, requestName, taskPath,
                blockMaxCloseTime, blockMaxFiles,
                blockMaxEvents, blockMaxSize,
                conn = None, transaction = False):
        """
        _execute_

        Insert a simple workflow into the dbsbuffer_workflow table
        """
        binds = {'name': requestName, 'task': taskPath,
                 'blockMaxCloseTime' : blockMaxCloseTime,
                 'blockMaxFiles' : blockMaxFiles,
                 'blockMaxEvents' : blockMaxEvents,
                 'blockMaxSize' : blockMaxSize}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        binds = {'name': requestName, 'task': taskPath}

        result = self.dbi.processData(self.existsSQL, binds, conn = conn,
                                      transaction = transaction)
        workflowId = self.formatDict(result)[0]['id']
        return workflowId
