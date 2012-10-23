#!/usr/bin/env python
"""
_GetBlock_

MySQL implementation of DBSBufferFiles.GetBlock
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetBlock(DBFormatter):
    sql = """SELECT blockname FROM dbsbuffer_block
               INNER JOIN dbsbuffer_file ON
                 dbsbuffer_block.id = dbsbuffer_file.block_id
             WHERE dbsbuffer_file.lfn = :lfn"""

    def execute(self, lfn = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"lfn": lfn},
                         conn = conn, transaction = transaction)
        return self.format(result)
