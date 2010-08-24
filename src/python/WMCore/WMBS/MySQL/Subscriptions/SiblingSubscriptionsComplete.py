#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

MySQL implementation of Subscription.SiblingSubscriptionsComplete
"""




from WMCore.Database.DBFormatter import DBFormatter

class SiblingSubscriptionsComplete(DBFormatter):
    # For each file in the input fileset count the number of subscriptions that
    # have completed the file and the number that have failed the file.  If the
    # number of subscritpions that have completed the file plus the number that
    # have failed the file are the same as the number of subscriptions that
    # processed this file (not counting this subscription) we can say that
    # processing of the file is complete and we can preform some other
    # action on it (usually deletion).
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.lfn, wmbs_location.se_name FROM wmbs_file_details
               INNER JOIN
                 (SELECT wmbs_fileset_files.file FROM wmbs_fileset_files
                    LEFT OUTER JOIN wmbs_sub_files_acquired ON
                      wmbs_fileset_files.file = wmbs_sub_files_acquired.file AND
                      wmbs_sub_files_acquired.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_complete ON
                      wmbs_fileset_files.file = wmbs_sub_files_complete.file AND
                      wmbs_sub_files_complete.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_failed ON
                      wmbs_fileset_files.file = wmbs_sub_files_failed.file AND
                      wmbs_sub_files_failed.subscription = :subscription
                  WHERE wmbs_sub_files_acquired.file IS Null AND
                        wmbs_sub_files_complete.file IS Null AND
                        wmbs_sub_files_failed.file IS Null AND
                        wmbs_fileset_files.fileset = :fileset) available_files ON
                 wmbs_file_details.id = available_files.file       
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_complete.file AS file, COUNT(file) AS complete_files
                    FROM wmbs_sub_files_complete
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_complete.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_complete.file) complete_files ON
                 wmbs_file_details.id = complete_files.file
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_failed.file AS file, COUNT(file) AS failed_files
                    FROM wmbs_sub_files_failed
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_failed.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_failed.file) failed_files ON
                 wmbs_file_details.id = failed_files.file
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.file
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE COALESCE(complete_files.complete_files, 0) +
                   COALESCE(failed_files.failed_files, 0) =
               (SELECT COUNT(*) FROM wmbs_subscription
                WHERE wmbs_subscription.id != :subscription AND
                      wmbs_subscription.fileset = :fileset)"""

    def execute(self, subscription, fileset, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription,
                                             "fileset": fileset},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
