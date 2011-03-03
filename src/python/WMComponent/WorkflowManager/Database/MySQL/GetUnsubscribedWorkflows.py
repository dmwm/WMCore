#!/usr/bin/env python
"""
_GetUnsubscribedWorkflows_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetUnsubscribedWorkflows(DBFormatter):

    sql = """
SELECT id, workflow, fileset_match, split_algo, type
FROM wm_managed_workflow
WHERE workflow NOT in (SELECT workflow FROM wmbs_subscription)
"""

    def execute(self, conn = None, transaction = False):
        """
        Get unsubscribed workflows
        """
        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
