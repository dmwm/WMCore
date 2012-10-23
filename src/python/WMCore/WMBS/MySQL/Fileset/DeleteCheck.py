#!/usr/bin/env python
"""
_DeleteFileset_

MySQL implementation of DeleteCheck

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class DeleteCheck(DBFormatter):
    sql = """DELETE FROM wmbs_fileset WHERE id = :fileset AND
           NOT EXISTS (SELECT id FROM wmbs_subscription WHERE fileset = :fileset AND id != :subscription)"""

    def execute(self, fileid, subid, conn = None, transaction = False):
        self.dbi.processData(self.sql, {'fileset': fileid, 'subscription': subid},
                             conn = conn, transaction = transaction)
        return True #or raise
