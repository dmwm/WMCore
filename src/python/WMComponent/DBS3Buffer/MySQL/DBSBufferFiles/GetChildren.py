#!/usr/bin/env python
"""
_GetChildren_

MySQL implementation of DBSBufferFiles.GetChildren
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetChildren(DBFormatter):
    sql = """SELECT lfn FROM dbsbuffer_file
               INNER JOIN dbsbuffer_file_parent ON
                 dbsbuffer_file.id = dbsbuffer_file_parent.child
             WHERE dbsbuffer_file_parent.parent =
               (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)"""

    def format(self, results):
        """
        _format_

        Turn the query results into a list of LFNs.
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
