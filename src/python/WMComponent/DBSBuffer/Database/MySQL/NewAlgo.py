#!/usr/bin/env python
"""
_NewAlgo_

MySQL implementation of DBSBuffer.NewAlgo
"""

__revision__ = "$Id: NewAlgo.py,v 1.4 2009/07/13 19:54:23 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

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

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return 
