#!/usr/bin/env python
"""
_GetBlock_

MySQL implementation of DBSBufferFiles.GetBlock
"""

__revision__ = "$Id: GetOpenBlocks.py,v 1.1 2009/12/07 21:57:27 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetOpenBlocks(DBFormatter):
    sql = """SELECT blockname as blockname, create_time as create_time FROM dbsbuffer_block
             WHERE status = 'Open'"""    
    
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
