#!/usr/bin/env python
"""
_CountBlocks_

MySQL implementation of DBSBuffer.CountBlocks
"""

__revision__ = "$Id: CountBlocks.py,v 1.1 2009/09/28 15:12:12 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class CountBlocks(DBFormatter):
    """
    _CountBlocks_

    Count the number of blocks in the buffer.  This is used by the DBSUpload
    unit test.
    """
    sql = "SELECT COUNT(*) FROM dbsbuffer_block"

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                          transaction = transaction)
        result = self.format(result)
        return result[0][0]
