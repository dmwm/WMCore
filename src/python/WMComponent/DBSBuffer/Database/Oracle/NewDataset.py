#!/usr/bin/env python
"""
_NewDataset_

Oracle implementation of DBSBuffer.NewDataset
"""




from WMCore.Database.DBFormatter import DBFormatter

class NewDataset(DBFormatter):
    sql = """INSERT INTO dbsbuffer_dataset (path, valid_status)
               SELECT :path, :valid_status FROM DUAL WHERE NOT EXISTS
                 (SELECT * FROM dbsbuffer_dataset WHERE path = :path)"""

    def execute(self, datasetPath, validStatus, conn = None, transaction = False):
        binds = {"path": datasetPath, 'valid_status': validStatus}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
