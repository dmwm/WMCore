#!/usr/bin/env python
"""
_SetDatasetAlgo_

MySQL implementation of DBSUpload.SetDatabaseAlgo
Should set the database-algo inDBS switch
"""

__revision__ = "$Id: SetDatasetAlgo.py,v 1.1 2010/02/24 21:41:34 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetDatasetAlgo(DBFormatter):

    sql = """UPDATE dbsbuffer_algo_dataset_assoc das
             SET das.in_dbs = :in_dbs
             WHERE das.id = :datasetAlgo
    """

    def execute(self, datasetAlgo, inDBS = 1, conn = None, transaction = False):
        binds = {"datasetAlgo": datasetAlgo, 'in_dbs': inDBS}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return 
