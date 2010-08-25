#!/usr/bin/env python
"""
_NewAlgo_

MySQL implementation of DBSBuffer.NewAlgo
"""




from WMCore.Database.DBFormatter import DBFormatter

class NewAlgo(DBFormatter):
    existsSQL = """SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                     app_ver = :app_ver AND app_fam = :app_fam AND
                     pset_hash = :pset_hash FOR UPDATE"""

    sql = """INSERT IGNORE INTO dbsbuffer_algo (app_name, app_ver, app_fam, pset_hash,
                                         config_content, in_dbs)
               VALUES (:app_name, :app_ver, :app_fam, :pset_hash,
                 :config_content, 0)"""

    def execute(self, appName, appVer, appFam, psetHash = None,
                configContent = None, conn = None, transaction = False):
        binds = {"app_name": appName, "app_ver": appVer, "app_fam": appFam,
                 "pset_hash": psetHash, "config_content": configContent}

        result = self.dbi.processData(self.existsSQL, binds, conn = conn,
                                      transaction = transaction)
        result = self.format(result)

        if len(result) == 0:
            self.dbi.processData(self.sql, binds, conn = conn,
                                 transaction = transaction)

        return 
