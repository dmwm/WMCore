#!/usr/bin/env python
"""
_MarkDatasetSubscribed_

"""




from WMCore.Database.DBFormatter import DBFormatter

class MarkDatasetSubscribed(DBFormatter):
    sql = "UPDATE dbsbuffer_dataset SET subscribed = 1 WHERE path = :path"

    def execute(self, datasetPath, conn = None, transaction = False):
        self.dbi.processData(self.sql, binds = {"path": datasetPath},
                             conn = conn, transaction = transaction)
        return
