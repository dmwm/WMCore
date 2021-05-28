"""
_MarkBlocksDeleted_

MySQL implementation of PhEDExInjector.MarkBlocksDeleted

Set deleted status for blocks

"""

from __future__ import division
from __future__ import print_function

from WMCore.Database.DBFormatter import DBFormatter


class MarkBlocksDeleted(DBFormatter):
    sql = """UPDATE dbsbuffer_block
             SET dbsbuffer_block.deleted = :DELETED
             WHERE dbsbuffer_block.blockname = :BLOCKNAME
             """

    def execute(self, binds, conn=None, transaction=False):

        if not binds:
            return

        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)

        return
