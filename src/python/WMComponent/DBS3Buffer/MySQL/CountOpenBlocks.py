#!/usr/bin/env python
"""
_CountOpenBlocks_

MySQL implementation of DBSBuffer.CountOpenBlocks

"""

from __future__ import division
from WMCore.Database.DBFormatter import DBFormatter


class CountOpenBlocks(DBFormatter):
    """
    _CountOpenBlocks_

    Count the number of open blocks. Used to report open blocks for drain status.

    """
    sql = """SELECT COUNT(*)
             FROM dbsbuffer_block
             WHERE status!='Closed'"""

    def execute(self, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, conn=conn,
                                      transaction=transaction)
        result = self.format(result)
        return result[0][0]
