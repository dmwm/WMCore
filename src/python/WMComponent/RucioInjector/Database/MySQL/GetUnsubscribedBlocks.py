#!/usr/bin/env python
"""
_GetUnsubscribedBlocks_

MySQL implementation of RucioInjector.Database.GetUnsubscribedBlocks
"""
from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class GetUnsubscribedBlocks(DBFormatter):
    """
    _GetUnsubscribedBlocks_

    Gets the unsubscribed blocks from DBSBuffer, so blocks
    without a valid rule id
    """
    sql = """SELECT dbsbuffer_block.blockname, dbsbuffer_location.pnn
               FROM dbsbuffer_block
             INNER JOIN dbsbuffer_location ON
                   dbsbuffer_block.location = dbsbuffer_location.id
             WHERE dbsbuffer_block.rule_id = '0'"""

    def execute(self, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, conn=conn,
                                      transaction=transaction)
        return self.formatDict(result)
