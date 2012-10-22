#!/usr/bin/env python
"""
_GetParentStatus_

MySQL implementation of DBSBufferFile.GetParentStatus
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetParentStatus(DBFormatter):
    sql = """SELECT status FROM dbsbuffer_file
               INNER JOIN dbsbuffer_file_parent ON
                 dbsbuffer_file.id = dbsbuffer_file_parent.parent
             WHERE dbsbuffer_file_parent.child =
               (SELECT id FROM dbsbuffer_file WHERE lfn  = :lfn)"""

    def format(self, results):
        """
        _format_

        Format the query results into a list of LFNs.
        """
        results = DBFormatter.format(self, results)

        status = []
        for result in results:
            status.append(result[0])
        return status

    def execute(self, lfn, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"lfn": lfn}, conn = conn,
                                      transaction = transaction)
        return self.format(result)
