#!/usr/bin/env python
"""
_SetBlock_

MySQL implementation of DBSBufferFiles.SetBlock
"""

from WMCore.Database.DBFormatter import DBFormatter
from sets import Set

class SetBlock(DBFormatter):
    sql = """UPDATE dbsbuffer_file SET block_id =
               (SELECT id FROM dbsbuffer_block WHERE blockname = :block)
             WHERE dbsbuffer_file.lfn = :filelfn"""  
    
    def execute(self, lfn, blockName, conn = None, transaction = None):
        result = self.dbi.processData(self.sql, {"block": blockName,
                                                 "filelfn": lfn},
                                      conn = conn,
                                      transaction = transaction)
        return
