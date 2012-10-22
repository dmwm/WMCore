#!/usr/bin/env python
"""
_GetBlock_

MySQL implementation of DBSBufferFiles.GetBlock
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetOpenBlocks(DBFormatter):
    sql = """SELECT DISTINCT blockname AS blockname, create_time AS create_time
             FROM dbsbuffer_block
             WHERE status = 'Open' OR status = 'Pending'  """

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
