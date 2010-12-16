#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

MySQL implementation of Subscription.SiblingSubscriptionsComplete
"""

from WMCore.Database.DBFormatter import DBFormatter

class SiblingSubscriptionsComplete(DBFormatter):
    # For each file in the input fileset count the number of subscriptions that
    # have completed the file.  If the number of subscriptions that have
    # completed the file is the same as the number of subscriptions that
    # processed the file (not counting this subscription) we can say that
    # processing of the file is complete and we can preform some other
    # action on it (usually deletion).
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.lfn, wmbs_location.se_name
                    FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_details ON
                 wmbs_sub_files_available.fileid = wmbs_file_details.id
               INNER JOIN
                 (SELECT wmbs_sub_files_complete.fileid AS fileid, COUNT(fileid) AS complete_files
                    FROM wmbs_sub_files_complete
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_complete.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_complete.fileid) complete_files ON
                 wmbs_file_details.id = complete_files.fileid
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.fileid
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE complete_files.complete_files =
               (SELECT COUNT(*) FROM wmbs_subscription
                WHERE wmbs_subscription.id != :subscription AND
                      wmbs_subscription.fileset = :fileset)"""

    def execute(self, subscription, fileset, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription,
                                             "fileset": fileset},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
