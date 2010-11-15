#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

Oracle implementation of Subscription.SiblingSubscriptionsComplete
"""

from WMCore.WMBS.MySQL.Subscriptions.SiblingSubscriptionsComplete import \
    SiblingSubscriptionsComplete as SiblingCompleteMySQL    

class SiblingSubscriptionsComplete(SiblingCompleteMySQL):
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.lfn, wmbs_location.se_name
                    FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_details ON
                 wmbs_sub_files_available.fileid = wmbs_file_details.id
               LEFT OUTER JOIN
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
             WHERE COALESCE(complete_files.complete_files, 0) =
               (SELECT COUNT(*) FROM wmbs_subscription
                WHERE wmbs_subscription.id != :subscription AND
                      wmbs_subscription.fileset = :fileset)"""
