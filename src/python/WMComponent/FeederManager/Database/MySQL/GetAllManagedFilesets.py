#!/usr/bin/env python
"""
_GetAllManagedFilesets_

MySQL implementation of FeederManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetAllManagedFilesets(DBFormatter):

    sql = """
SELECT id, name from wmbs_fileset
WHERE EXISTS (SELECT 1 FROM managed_filesets WHERE managed_filesets.fileset = wmbs_fileset.id )
            """

    def execute(self, conn = None, transaction = False):
        """
        Get all managed filesets
        """
        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
