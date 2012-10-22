#!/usr/bin/env python
"""
_GetUnsubscribedFilesets_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetUnsubscribedFilesets(DBFormatter):

    sql = """
SELECT wmbs_fileset.id, wmbs_fileset.name
FROM wmbs_fileset
WHERE NOT EXISTS (SELECT 1 FROM wmbs_subscription
                  WHERE wmbs_subscription.fileset = wmbs_fileset.id)
"""

    def execute(self, conn = None, transaction = False):
        """
        Get unsubscribed filesets
        """
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
