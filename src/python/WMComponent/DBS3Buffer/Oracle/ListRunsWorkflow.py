#!/usr/bin/env python
"""
_ListRunsWorkflow_

For a given workflow, list the processed runs according to DBSBUFFER tables
"""

from future.utils import viewvalues

import threading

from WMCore.Database.DBFormatter import DBFormatter

class ListRunsWorkflow(DBFormatter):

    sql = """SELECT DISTINCT dbsbuffer_file_runlumi_map.run FROM
                dbsbuffer_file_runlumi_map INNER JOIN dbsbuffer_file
                ON dbsbuffer_file_runlumi_map.filename = dbsbuffer_file.id
                INNER JOIN dbsbuffer_workflow
                ON dbsbuffer_file.workflow = dbsbuffer_workflow.id
                WHERE dbsbuffer_workflow.name = :workflow"""

    def execute(self, workflow = None, conn = None, transaction = False):
        """
        _execute_

        Changed to expect a DBSBlock object
        """
        bindVars = []

        bindVars.append({"workflow": workflow})

        results = self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        runs = []

        for result in results[0].fetchall():
            runs.append(next(iter(viewvalues(result))))

        return runs
