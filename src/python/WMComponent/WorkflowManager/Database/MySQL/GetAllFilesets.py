#!/usr/bin/env python
"""
_GetAllFilesets_

MySQL implementation of WorkflowManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetAllFilesets(DBFormatter):

    sql = """
SELECT wmbs_fileset.id, wmbs_fileset.name
FROM wmbs_fileset
"""

    def execute(self, conn = None, transaction = False):
        """
        Get all filesets
        """
        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
