#!/usr/bin/env python
"""
_SetDatasetAlgo_

MySQL implementation of DBSUpload.SetDatabaseAlgo
Should set the database-algo inDBS switch
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetDatasetAlgo(DBFormatter):
    """
    Set the inDBS status of a datasetAlgo

    """

    sql = """UPDATE dbsbuffer_algo_dataset_assoc das
             SET das.in_dbs = :in_dbs
             WHERE das.id = :datasetAlgo
    """

    def execute(self, datasetAlgo, inDBS = 1, conn = None, transaction = False):
        """
        _execute_

        Set the datasetAlgo
        """
        binds = {"datasetAlgo": datasetAlgo, 'in_dbs': inDBS}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
