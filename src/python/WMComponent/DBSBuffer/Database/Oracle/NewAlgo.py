#!/usr/bin/env python
"""
_NewAlgo_

Oracle implementation of DBSBuffer.NewAlgo
"""




import logging

from sqlalchemy.exceptions import IntegrityError
from WMCore.Database.DBFormatter import DBFormatter

class NewAlgo(DBFormatter):
    """
    _NewAlgo_

    Add a new algorithm to the DBSBuffer.  This will do nothing if an algorithm
    with the given parameters already exists.
    """
    sql = """INSERT INTO dbsbuffer_algo (app_name, app_ver, app_fam, pset_hash,
                                         config_content, in_dbs)
               SELECT :app_name, :app_ver, :app_fam, :pset_hash,
                 :config_content, 0 FROM DUAL WHERE NOT EXISTS
                   (SELECT * FROM dbsbuffer_algo WHERE app_name = :app_name AND
                      app_ver = :app_ver AND app_fam = :app_fam AND
                      pset_hash = :pset_hash)"""

    def execute(self, appName = None, appVer = None, appFam = None,
                psetHash = None, configContent = None, conn = None,
                transaction = False):
        binds = {"app_name": appName, "app_ver": appVer, "app_fam": appFam,
                 "pset_hash": psetHash, "config_content": configContent}

        try:
            self.dbi.processData(self.sql, binds, conn = conn,
                                 transaction = transaction)
        except Exception, ex:
            if "orig" in dir(ex) and type(ex.orig) != tuple:
                if str(ex.orig).find("ORA-00001: unique constraint") != -1 and \
                   str(ex.orig).find("DBSBUFFER_ALGO_UNIQUE") != -1:
                    return
            raise ex

        return
