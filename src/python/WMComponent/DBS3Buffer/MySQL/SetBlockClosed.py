#!/usr/bin/env python
"""
_DBS3Buffer.SetBlockClosed_

Update block status to Closed

"""

from WMCore.Database.DBFormatter import DBFormatter

class SetBlockClosed(DBFormatter):

    def execute(self, block, conn = None, transaction = False):

        sql = """UPDATE dbsbuffer_block
                 SET status = 'Closed'
                 WHERE blockname = :block
                 """

        self.dbi.processData(sql, { 'block' : block },
                             conn = conn, transaction = transaction)

        return
