#!/usr/bin/env python
"""
_NewAlgo_

Oracle implementation of DBSBuffer.NewAlgo
"""

from WMComponent.DBS3Buffer.MySQL.NewAlgo import NewAlgo as MySQLNewAlgo

class NewAlgo(MySQLNewAlgo):

    sql = """INSERT INTO dbsbuffer_algo
             (app_name, app_ver, app_fam, pset_hash, config_content, in_dbs)
             SELECT :app_name, :app_ver, :app_fam,
                    :pset_hash, :config_content, 0
             FROM DUAL
             WHERE NOT EXISTS
               ( SELECT *
                 FROM dbsbuffer_algo
                 WHERE app_name = :app_name
                 AND app_ver = :app_ver
                 AND app_fam = :app_fam
                 AND pset_hash = :pset_hash )
             """

