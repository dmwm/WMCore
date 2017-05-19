#!/usr/bin/env python
"""
_AddIgnore_

MySQL implementation of DBSBufferFiles.AddIgnore
"""

from WMCore.Database.DBFormatter import DBFormatter


class AddIgnore(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_file (lfn, dataset_algo, status, in_phedex)
                VALUES (:lfn, :dataset_algo, :status, :in_phedex)"""

    def execute(self, lfns, datasetAlgo, status, inPhEDEx,
                conn=None, transaction=False):
        binds = []
        for lfn in lfns:
            binds.append({"lfn": lfn, "dataset_algo": datasetAlgo,
                          "status": status, "in_phedex": inPhEDEx})

        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)
        return
