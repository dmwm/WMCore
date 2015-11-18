#!/usr/bin/env python
"""
_NewAlgo_

MySQL implementation of DBSBuffer.NewAlgo
"""




from WMCore.Database.DBFormatter import DBFormatter

class NewAlgo(DBFormatter):

    sql = """INSERT IGNORE INTO dbsbuffer_algo
             (app_name, app_ver, app_fam, pset_hash, config_content, in_dbs)
             VALUES (:app_name, :app_ver, :app_fam, :pset_hash, :config_content, 0)
             """

    def execute(self, appName, appVer, appFam, psetHash = None,
                configContent = None, conn = None, transaction = False):
        """
        _execute_

        Add a new algo into the database
        """
        binds = {"app_name": appName, "app_ver": appVer, "app_fam": appFam,
                 "pset_hash": psetHash, "config_content": configContent}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        return
