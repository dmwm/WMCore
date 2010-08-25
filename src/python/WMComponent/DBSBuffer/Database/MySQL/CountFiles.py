#!/usr/bin/env python
"""
_CountFiles_

MySQL implementation of DBSBuffer.CountFiles
"""




from WMCore.Database.DBFormatter import DBFormatter

class CountFiles(DBFormatter):
    """
    _CountFiles_

    Count the number of files in the buffer.  This is used by the JobAccountant
    unit test.
    """
    sql = "SELECT COUNT(*) FROM dbsbuffer_file"

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        result = self.format(result)
        return result[0][0]
