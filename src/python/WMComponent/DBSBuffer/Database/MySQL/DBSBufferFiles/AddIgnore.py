#!/usr/bin/env python
"""
_AddIgnore_

MySQL implementation of DBSBufferFiles.AddIgnore
"""

from WMCore.Database.DBFormatter import DBFormatter

class AddIgnore(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_file (lfn, dataset_algo, status) 
                VALUES (:lfn, :dataset_algo, :status)"""
                
    def execute(self, lfns, datasetAlgo, status,
                conn = None, transaction = False):
        binds = []
        for lfn in lfns:
            binds.append({"lfn": lfn, "dataset_algo": datasetAlgo,
                          "status": status})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
