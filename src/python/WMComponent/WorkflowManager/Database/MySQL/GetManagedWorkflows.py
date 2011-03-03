#!/usr/bin/env python
"""
_GetManagedWorkflows_

MySQL implementation of WorkflowManager backend.
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetManagedWorkflows(DBFormatter):

    sql = """
SELECT id, workflow, fileset_match, split_algo, type
FROM wm_managed_workflow
"""
    def execute(self, conn = None, transaction = False):
        """
        Get managed workflows
        """
        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
