#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

MySQL implementation of Subscription.SiblingSubscriptionsComplete
"""

__revision__ = "$Id: SiblingSubscriptionsComplete.py,v 1.1 2010/04/22 15:42:40 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SiblingSubscriptionsComplete(DBFormatter):
    # For each file in the input fileset count the number of subscriptions that
    # have completed the file and the number that have failed the file.  If the
    # number of subscritpions that have completed the file plus the number that
    # have failed the file are the same (not counting this subscription) we can
    # say that processing of the file is complete and we can preform some other
    # action on it (usually deletion).
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.lfn FROM wmbs_file_details
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_details.id = wmbs_fileset_files.file AND
                 wmbs_fileset_files.fileset = :fileset
               LEFT OUTER JOIN wmbs_sub_files_acquired this_sub_acquired ON
                 wmbs_file_details.id = this_sub_acquired.file AND
                 this_sub_acquired.subscription = :subscription
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_complete.file AS file, COUNT(*) AS complete_files
                    FROM wmbs_sub_files_complete
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_complete.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_complete.file) complete_files ON
                 wmbs_file_details.id = complete_files.file
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_failed.file AS file, COUNT(*) AS failed_files
                    FROM wmbs_sub_files_failed
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_failed.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_failed.file) failed_files ON
                 wmbs_file_details.id = failed_files.file
             WHERE COALESCE(complete_files.complete_files, 0) +
                   COALESCE(failed_files.failed_files, 0) =
               (SELECT COUNT(*) FROM wmbs_subscription
                WHERE wmbs_subscription.id != :subscription AND
                      wmbs_subscription.fileset = :fileset) AND
               this_sub_acquired.file IS Null"""

    def execute(self, subscription, fileset, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription,
                                             "fileset": fileset},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
