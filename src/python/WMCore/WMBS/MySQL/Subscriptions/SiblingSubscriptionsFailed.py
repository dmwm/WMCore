#!/usr/bin/env python
"""
_SiblingSubscriptionsFailed_

MySQL implementation of Subscription.SiblingSubscriptionsFailed
"""

from WMCore.Database.DBFormatter import DBFormatter

class SiblingSubscriptionsFailed(DBFormatter):
    # Find files that have failed processing and mark them as complete for the
    # cleanup subscription.  These files should not be cleaned up as they may be
    # used to investigate why the job failed.  This should only be run after the
    # input fileset has been closed.
    sql = """SELECT DISTINCT wmbs_fileset_files.fileid AS fileid,
                             :subscription AS subscription FROM wmbs_fileset_files
               INNER JOIN wmbs_subscription ON
                 wmbs_fileset_files.fileset = wmbs_subscription.fileset
               INNER JOIN wmbs_sub_files_failed ON
                 wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid AND
                 wmbs_subscription.id = wmbs_sub_files_failed.subscription
             WHERE wmbs_fileset_files.fileset = :fileset"""

    delete = """DELETE FROM wmbs_sub_files_available
                WHERE subscription = :subscription AND
                      fileid = :fileid"""

    insert = """INSERT IGNORE INTO wmbs_sub_files_complete (subscription, fileid) VALUES
                  (:subscription, :fileid)"""

    def execute(self, subscription, fileset, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"fileset": fileset,
                                                  "subscription": subscription},
                                       conn = conn, transaction = transaction)
        failedFiles = self.formatDict(results)

        if len(failedFiles) > 0:
            self.dbi.processData(self.delete, failedFiles, conn = conn,
                                 transaction = transaction)
            self.dbi.processData(self.insert, failedFiles, conn = conn,
                                 transaction = transaction)
        return
