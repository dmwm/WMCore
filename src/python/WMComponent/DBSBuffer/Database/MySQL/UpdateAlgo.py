#!/usr/bin/env python
"""
_UpdateAlgo_

MySQL implementation of DBSBuffer.UpdateAlgo
"""




from WMCore.Database.DBFormatter import DBFormatter

class UpdateAlgo(DBFormatter):
    sql = """UPDATE dbsbuffer_algo SET in_dbs = :in_dbs
             WHERE app_name = :app_name AND app_ver = :app_ver AND
                   app_fam = :app_fam AND pset_hash = :pset_hash"""

    sqlID = """UPDATE dbsbuffer_algo SET in_dbs = :in_dbs
               WHERE id = :algoID"""

    def execute(self, inDBS, algoID = None, appName = None, appVer = None,
                appFam = None, psetHash = None, conn = None,
                transaction = False):
        binds = {"in_dbs": inDBS}

        if algoID == None:
            binds["app_name"] = appName
            binds["app_ver"] = appVer
            binds["app_fam"] = appFam
            binds["pset_hash"] = psetHash
            self.dbi.processData(self.sql, binds, conn = conn,
                                 transaction = transaction)
        else:
            binds["algoID"] = algoID
            self.dbi.processData(self.sqlID, binds, conn = conn,
                                 transaction = transaction)

        return
