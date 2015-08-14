#!/usr/bin/env python
"""
_SetBlock_

MySQL implementation of DBSBufferFiles.SetBlock
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetBlock(DBFormatter):
    sql = """UPDATE dbsbuffer_file SET block_id =
               (SELECT id FROM dbsbuffer_block WHERE blockname = :block)
             WHERE dbsbuffer_file.lfn = :filelfn"""

    def execute(self, lfn, blockName, conn = None, transaction = None):
        """
        Insert block name into files

        """

        if isinstance(lfn, list):
            binds = []
            for entry in lfn:
                binds.append({'block': blockName, 'filelfn': entry})
        else:
            binds = {"block": blockName, "filelfn": lfn}


        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return
