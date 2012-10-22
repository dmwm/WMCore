#!/usr/bin/env python
"""
_SetBlockFiles_

MySQL implementation of DBSBufferFiles.SetBlock
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetBlockFiles(DBFormatter):
    """
    Update files with block info
    """
    sql = """UPDATE dbsbuffer_file SET block_id =
               (SELECT id FROM dbsbuffer_block WHERE blockname = :block)
             WHERE dbsbuffer_file.lfn = :filelfn"""

    def execute(self, binds, conn = None, transaction = None):
        """
        Insert block name into files

        Requires you to pass in binds correctly
        """

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
