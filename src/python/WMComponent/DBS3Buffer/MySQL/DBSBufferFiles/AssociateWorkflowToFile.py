#!/usr/bin/env python
"""
_AssociateWorkflowToFile_

MySQL implementation of DBS3Buffer.DBSBufferFiles.AssociateWorkflowToFile

Created on Oct 12, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class AssociateWorkflowToFile(DBFormatter):
    """
    _AssociateWorkflowToFile_

    Associate the given workflow and task to an existing dbsbuffer file,
    the lfn of an existing file in dbsbuffer is the input
    """

    sql = """UPDATE dbsbuffer_file
             SET workflow = (SELECT id FROM dbsbuffer_workflow
                             WHERE name = :name AND task = :task)
             WHERE lfn = :lfn
          """

    def execute(self, lfn, workflowName, task, conn = None, transaction = False):
        """
        _execute_

        Run the query
        """
        binds = {"lfn" : lfn, "name" : workflowName, "task" : task}

        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)

        return
