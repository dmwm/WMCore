#!/usr/bin/env python
"""
_AddIgnore_

MySQL implementation of DBSBufferFiles.AddIgnore
"""

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddIgnore import AddIgnore as \
    MySQLAddIgnore


class AddIgnore(MySQLAddIgnore):
    sql = """INSERT INTO dbsbuffer_file (lfn, dataset_algo, status, in_phedex)
                SELECT :lfn, :dataset_algo, :status, :in_phedex FROM DUAL WHERE NOT EXISTS
                  (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)"""
