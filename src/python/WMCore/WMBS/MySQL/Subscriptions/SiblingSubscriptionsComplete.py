#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

MySQL implementation of Subscription.SiblingSubscriptionsComplete
"""

from WMCore.Database.DBFormatter import DBFormatter


class SiblingSubscriptionsComplete(DBFormatter):
    """
    For each file in the input fileset count the number of subscriptions
    (on the same input fileset) that have completed the file. If the number
    of subscriptions that have completed the file is the same as the number
    of subscriptions that processed the file (not counting this subscription)
    we can say that processing of the file is complete and we can perform
    some other action on it (usually deletion).

    """
    sql = """SELECT wmbs_file_details.id,
                    wmbs_file_details.events,
                    wmbs_file_details.lfn,
                    wmbs_pnns.pnn
             FROM (
               SELECT wmbs_sub_files_available.fileid
               FROM wmbs_sub_files_available
                 INNER JOIN wmbs_subscription ON
                   wmbs_subscription.id = wmbs_sub_files_available.subscription
                 LEFT OUTER JOIN wmbs_subscription sibling_subscription ON
                   sibling_subscription.fileset = wmbs_subscription.fileset AND
                   sibling_subscription.id != wmbs_subscription.id
                 LEFT OUTER JOIN wmbs_sub_files_complete ON
                   wmbs_sub_files_complete.fileid = wmbs_sub_files_available.fileid AND
                   wmbs_sub_files_complete.subscription = sibling_subscription.id
               WHERE wmbs_sub_files_available.subscription = :subscription
               GROUP BY wmbs_sub_files_available.fileid
               HAVING COUNT(sibling_subscription.id) = COUNT(wmbs_sub_files_complete.fileid)
             ) available_files
             INNER JOIN wmbs_file_details ON
               wmbs_file_details.id = available_files.fileid
             INNER JOIN wmbs_file_location ON
               wmbs_file_location.fileid = available_files.fileid
             INNER JOIN wmbs_pnns ON
                 wmbs_file_location.pnn = wmbs_pnns.id
             """

    def execute(self, subscription, conn=None, transaction=False):
        results = self.dbi.processData(self.sql, {'subscription': subscription},
                                       conn=conn, transaction=transaction)

        return self.formatDict(results)
