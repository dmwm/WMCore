#!/usr/bin/env python
"""
_AddToFileset_

MySQL implementation of Files.AddToFileset
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class AddToFileset(DBFormatter):
    sql = """INSERT INTO wmbs_fileset_files (file, fileset, insert_time)
               SELECT wmbs_file_details.id, :fileset, :insert_time
               FROM wmbs_file_details
               WHERE wmbs_file_details.lfn = :lfn
               AND NOT EXISTS (SELECT file FROM wmbs_fileset_files wff2 WHERE
                                wff2.file = wmbs_file_details.id
                                AND wff2.fileset = :fileset)"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, file)
                    SELECT wmbs_subscription.id AS subscription,
                           wmbs_file_details.id AS file FROM wmbs_subscription
                      INNER JOIN wmbs_file_details ON
                        wmbs_file_details.lfn = :lfn
                    WHERE wmbs_subscription.fileset = :fileset"""
        
    def execute(self, file = None, fileset = None, conn = None,
                transaction = False):
        binds = []
        availBinds = []
        timestamp = int(time.time())
        for fileLFN in file:
            binds.append({"lfn": fileLFN, "fileset": fileset,
                          "insert_time": timestamp})
            availBinds.append({"lfn": fileLFN, "fileset": fileset})
            
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sqlAvail, availBinds, conn = conn,
                             transaction = transaction)        
        return
