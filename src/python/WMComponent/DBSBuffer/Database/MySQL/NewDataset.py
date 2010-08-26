#!/usr/bin/env python
"""
_NewDataset_

MySQL implementation of DBSBuffer.NewDataset
"""

__revision__ = "$Id: NewDataset.py,v 1.10 2009/10/22 15:08:15 sfoulkes Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.Database.DBFormatter import DBFormatter

class NewDataset(DBFormatter):
    existsSQL = "SELECT id FROM dbsbuffer_dataset WHERE path = :path FOR UPDATE"
    sql = "INSERT IGNORE INTO dbsbuffer_dataset (path) VALUES (:path)"

    def execute(self, datasetPath, conn = None, transaction = False):
        binds = {"path": datasetPath}

        result = self.dbi.processData(self.existsSQL, binds, conn = conn,
                                      transaction = transaction)
        result = self.format(result)

        if len(result) == 0:
            self.dbi.processData(self.sql, binds, conn = conn,
                                 transaction = transaction)

        return 
