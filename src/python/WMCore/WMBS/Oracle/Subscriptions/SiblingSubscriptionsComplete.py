#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

Oracle implementation of Subscription.SiblingSubscriptionsComplete
"""

__revision__ = "$Id: SiblingSubscriptionsComplete.py,v 1.1 2010/04/22 15:42:40 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.SiblingSubscriptionsComplete import \
    SiblingSubscriptionsComplete as SiblingCompleteMySQL    

class SiblingSubscriptionsComplete(SiblingCompleteMySQL):
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.lfn FROM wmbs_file_details
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_details.id = wmbs_fileset_files.fileid AND
                 wmbs_fileset_files.fileset = :fileset
               LEFT OUTER JOIN wmbs_sub_files_acquired this_sub_acquired ON
                 wmbs_file_details.id = this_sub_acquired.fileid AND
                 this_sub_acquired.subscription = :subscription
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_complete.fileid AS fileid, COUNT(*) AS complete_files
                    FROM wmbs_sub_files_complete
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_complete.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_complete.fileid) complete_files ON
                 wmbs_file_details.id = complete_files.fileid
               LEFT OUTER JOIN
                 (SELECT wmbs_sub_files_failed.fileid AS fileid, COUNT(*) AS failed_files
                    FROM wmbs_sub_files_failed
                    INNER JOIN wmbs_subscription ON
                      wmbs_sub_files_failed.subscription = wmbs_subscription.id AND
                      wmbs_subscription.fileset = :fileset
                  GROUP BY wmbs_sub_files_failed.fileid) failed_files ON
                 wmbs_file_details.id = failed_files.fileid
             WHERE COALESCE(complete_files.complete_files, 0) +
                   COALESCE(failed_files.failed_files, 0) =
               (SELECT COUNT(*) FROM wmbs_subscription
                WHERE wmbs_subscription.id != :subscription AND
                      wmbs_subscription.fileset = :fileset) AND
               this_sub_acquired.fileid IS Null"""
