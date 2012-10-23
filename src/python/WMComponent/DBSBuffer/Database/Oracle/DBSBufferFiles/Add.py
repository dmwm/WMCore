#!/usr/bin/env python
"""
Oracle implementation of AddFile
"""

#This has been modified for Oracle




from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Add import Add as MySQLAdd

class Add(MySQLAdd):
    """
    Oracle implementation of AddFile
    """

    sql = """insert into dbsbuffer_file(lfn, filesize, events, dataset_algo, status)
               SELECT :lfn, :filesize, :events, :dataset_algo, :status FROM dual
               WHERE NOT EXISTS (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)"""
