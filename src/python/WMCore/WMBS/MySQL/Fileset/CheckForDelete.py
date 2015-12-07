#!/usr/bin/env python
"""
_CheckForDelete_

MySQL implementation of DeleteCheck

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class CheckForDelete(DBFormatter):
    sql = """SELECT wf.id FROM wmbs_fileset wf
               WHERE wf.id = :fileset
               AND NOT EXISTS (SELECT id FROM wmbs_subscription ws
                               WHERE ws.fileset = :fileset
                               AND ws.id != :subscription)
                               """

    def execute(self, fileids = None, subid = None, conn = None, transaction = False):
        """
        _execute_

        Given a list of fileset IDs, figure out which ones can be deleted
        """
        binds = []
        for id in fileids:
            binds.append({'fileset': id, 'subscription': subid})

        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
