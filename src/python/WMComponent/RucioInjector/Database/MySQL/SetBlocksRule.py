#!/usr/bin/env python
"""
_SetBlocksRule_

MySQL implementation of RucioInjector.Database.SetBlocksRule
"""

from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class SetBlocksRule(DBFormatter):
    """
    _SetBlocksRule_

    Set rules ID for blocks in the DBSBuffer
    """
    sql = """UPDATE dbsbuffer_block
               SET dbsbuffer_block.rule_id = :RULE_ID
             WHERE dbsbuffer_block.blockname = :BLOCKNAME
             """

    def execute(self, binds, conn=None, transaction=False):
        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)

        return
