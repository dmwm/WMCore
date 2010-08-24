#!/usr/bin/env python
"""
_BulkAdd_

Oracle implementation of Fileset.BulkAdd
"""




from WMCore.WMBS.MySQL.Fileset.BulkAdd import BulkAdd as BulkAddFilesetMySQL

class BulkAdd(BulkAddFilesetMySQL):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               VALUES (:fileid, :fileset, :timestamp)"""
