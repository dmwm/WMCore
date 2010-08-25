#!/usr/bin/env python
"""
_NewDataset_

Oracle implementation of DBSBuffer.NewDataset
"""

__revision__ = "$Id: NewDataset.py,v 1.3 2009/10/22 14:49:45 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

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
