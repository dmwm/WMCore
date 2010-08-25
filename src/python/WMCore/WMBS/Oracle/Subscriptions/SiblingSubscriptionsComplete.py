#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

Oracle implementation of Subscription.SiblingSubscriptionsComplete
"""

from WMCore.WMBS.MySQL.Subscriptions.SiblingSubscriptionsComplete import \
    SiblingSubscriptionsComplete as SiblingCompleteMySQL    

class SiblingSubscriptionsComplete(SiblingCompleteMySQL):
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.lfn, wmbs_location.se_name FROM wmbs_file_details
               INNER JOIN
                 (SELECT wmbs_fileset_files.fileid FROM wmbs_fileset_files
                    LEFT OUTER JOIN wmbs_sub_files_acquired ON
                      wmbs_fileset_files.fileid = wmbs_sub_files_acquired.fileid AND
                      wmbs_sub_files_acquired.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_complete ON
                      wmbs_fileset_files.fileid = wmbs_sub_files_complete.fileid AND
                      wmbs_sub_files_complete.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_failed ON
                      wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid AND
                      wmbs_sub_files_failed.subscription = :subscription
                  WHERE wmbs_sub_files_acquired.fileid IS Null AND
                        wmbs_sub_files_complete.fileid IS Null AND
                        wmbs_sub_files_failed.fileid IS Null AND
                        wmbs_fileset_files.fileset = :fileset) available_files ON
                 wmbs_file_details.id = available_files.fileid       
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_complete.fileid AS fileid, COUNT(fileid) AS complete_files
                    FROM wmbs_sub_files_complete
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_complete.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_complete.fileid) complete_files ON
                 wmbs_file_details.id = complete_files.fileid
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_failed.fileid AS fileid, COUNT(fileid) AS failed_files
                    FROM wmbs_sub_files_failed
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_failed.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_failed.fileid) failed_files ON
                 wmbs_file_details.id = failed_files.fileid
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.fileid
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE COALESCE(complete_files.complete_files, 0) +
                   COALESCE(failed_files.failed_files, 0) =
               (SELECT COUNT(*) FROM wmbs_subscription
                WHERE wmbs_subscription.id != :subscription AND
                      wmbs_subscription.fileset = :fileset)"""
