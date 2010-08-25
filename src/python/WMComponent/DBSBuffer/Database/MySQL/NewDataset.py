#!/usr/bin/env python
"""
_NewDataset_

MySQL implementation of DBSBuffer.NewDataset
"""

__revision__ = "$Id: NewDataset.py,v 1.9 2009/07/13 19:53:44 sfoulkes Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter

class NewDataset(DBFormatter):
    sql = """INSERT INTO dbsbuffer_dataset (path)
               SELECT :path FROM DUAL WHERE NOT EXISTS
                 (SELECT * FROM dbsbuffer_dataset WHERE path = :path)"""

    def execute(self, datasetPath, conn = None, transaction = False):
        binds = {"path": datasetPath}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return 
