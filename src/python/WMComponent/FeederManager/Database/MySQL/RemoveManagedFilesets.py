#!/usr/bin/env python
"""
_RemoveManagedFilesets_
MySQL implementation of FeederManager backend.
"""

from WMCore.Database.DBFormatter import DBFormatter

class RemoveManagedFilesets(DBFormatter):

    sql = """
DELETE FROM managed_filesets
WHERE fileset = :fileset
AND feeder = :feederType
"""

    def getBinds(self, filesetId = '', feederType = ''):
        """
        Bind paramter
        """
        dict = {'fileset': filesetId,
                'feederType' : feederType
                 }
        return dict

    def execute(self, filesetId = '', feederType = '', conn = None, transaction = False):
        """
        Remover filesets from management given the feeder id and the type
        """
        binds = self.getBinds(filesetId, feederType)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return result
