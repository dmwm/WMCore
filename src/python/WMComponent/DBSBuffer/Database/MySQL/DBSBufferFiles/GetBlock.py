#!/usr/bin/env python
"""
_GetBlock_

MySQL implementation of DBSBufferFiles.GetBlock
"""

__revision__ = "$Id: GetBlock.py,v 1.1 2009/09/22 19:50:34 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

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
