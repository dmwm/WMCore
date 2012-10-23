#!/usr/bin/env python
"""
_ListAlgo_

MySQL implementation of DBSBuffer.ListAlgo
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListAlgo(DBFormatter):
    """
    _ListAlgo_

    Retrieve information about an algorithm from the DBSBuffer.  This is mainly
    used by the unit tests to verify that the NewAlgo DAO is working correctly.
    """
    sql = """SELECT id, app_name, app_ver, app_fam, pset_hash, config_content,
                    in_dbs FROM dbsbuffer_algo
               WHERE app_name = :app_name AND app_ver = :app_ver AND
                     app_fam = :app_fam AND pset_hash = :pset_hash"""

    sqlID = """SELECT id, app_name, app_ver, app_fam, pset_hash, config_content,
                      in_dbs FROM dbsbuffer_algo
                 WHERE id = :id"""

    def execute(self, algoID = None, appName = None, appVer = None, appFam = None,
                psetHash = None, configContent = None, conn = None,
                transaction = False):
        """
        _execute_

        Either the algorithm's ID or the appName, appVer, appFam and psetHash
        must be passed in.
        """
        if algoID == None:
            binds = {"app_name": appName, "app_ver": appVer, "app_fam": appFam,
                     "pset_hash": psetHash}
            result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)
        else:
            binds = {"id": algoID}
            result = self.dbi.processData(self.sqlID, binds, conn = conn,
                                          transaction = transaction)

        return self.formatDict(result)
