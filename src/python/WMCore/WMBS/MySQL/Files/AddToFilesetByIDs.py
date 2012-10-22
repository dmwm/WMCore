#!/usr/bin/env python
"""
_AddToFilesetByIDs_

MySQL implementation of Files.AddToFilesetByIDs
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class AddToFilesetByIDs(DBFormatter):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT :file_id, wmbs_fileset.id, :insert_time
                 FROM wmbs_fileset WHERE wmbs_fileset.id = :fileset"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription, :fileid
                           FROM wmbs_subscription
                    WHERE wmbs_subscription.fileset = :fileset"""

    def execute(self, file = None, fileset = None, conn = None, transaction = False):
        binds = []
        availBinds = []
        timestamp = int(time.time())
        for fileID in file:
            binds.append({"file_id": fileID, "fileset": fileset,
                          "insert_time": timestamp})
            availBinds.append({"fileid": fileID, "fileset": fileset})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sqlAvail, availBinds, conn = conn,
                             transaction = transaction)
        return
