#!/usr/bin/env python
"""
Oracle implementation of AddFile
"""





from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.Add import Add as MySQLAdd

class Add(MySQLAdd):
    """
    Oracle implementation of AddFile
    """
    sql = """INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX (dbsbuffer_file (lfn)) */ 
                 INTO dbsbuffer_file (lfn, filesize, events, dataset_algo, status, workflow, in_phedex)
                      values (:lfn, :filesize, :events, :dataset_algo, :status, :workflow, :in_phedex)"""
