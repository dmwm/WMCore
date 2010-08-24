#!/usr/bin/env python
"""
_AddIgnore_

MySQL implementation of DBSBufferFiles.AddIgnore
"""




from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddIgnore import AddIgnore as \
     MySQLAddIgnore

class AddIgnore(MySQLAddIgnore):
    sql = """INSERT INTO dbsbuffer_file (lfn, dataset_algo, status) 
                SELECT :lfn, :dataset_algo, :status FROM DUAL WHERE NOT EXISTS
                  (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)"""
