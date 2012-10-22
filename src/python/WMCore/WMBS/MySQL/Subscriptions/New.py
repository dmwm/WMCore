#!/usr/bin/env python
"""
_Subscription.New_

MySQL implementation of Subscription.New
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    _New_

    Create a new subscription.  Add any files that exist in the fileset to the
    wmbs_sub_files_available table.
    """
    typesSQL = """INSERT IGNORE INTO wmbs_sub_types (name)
                    VALUES (:subtype)"""

    sql = """INSERT INTO wmbs_subscription (fileset, workflow, subtype,
                                            split_algo, last_update)
               SELECT :fileset, :workflow, id, :split_algo, :timestamp
                      FROM wmbs_sub_types WHERE name = :subtype"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id, wmbs_fileset_files.fileid
                           FROM wmbs_fileset_files
                      INNER JOIN wmbs_subscription ON
                        wmbs_subscription.workflow = :workflow AND
                        wmbs_subscription.fileset = :fileset AND
                        wmbs_fileset_files.fileset = wmbs_subscription.fileset"""

    def execute(self, fileset = None, workflow = None, split_algo = "File",
                type = "Processing", conn = None, transaction = False):
        binds = {"fileset": fileset, "workflow": workflow, "subtype": type,
                 "split_algo": split_algo, "timestamp": int(time.time())}
        availBinds = {"fileset": fileset, "workflow": workflow}

        self.dbi.processData(self.typesSQL, {'subtype': type}, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sqlAvail, availBinds, conn = conn,
                             transaction = transaction)
        return
