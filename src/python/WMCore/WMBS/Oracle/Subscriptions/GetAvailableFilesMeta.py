#!/usr/bin/env python
"""
_GetAvailableFilesMeta_

Oracle implementation of Subscription.GetAvailableFilesMeta
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesMeta import \
     GetAvailableFilesMeta as GetAvailableFilesMetaMySQL

class GetAvailableFilesMeta(GetAvailableFilesMetaMySQL):
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.lfn, wmbs_file_details.filesize,
                    wmbs_file_details.events, MIN(wmbs_file_runlumi_map.run) AS run
                    FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_details ON
                 wmbs_sub_files_available.fileid = wmbs_file_details.id
               INNER JOIN wmbs_file_runlumi_map
                 ON wmbs_file_details.id = wmbs_file_runlumi_map.fileid
               WHERE wmbs_sub_files_available.subscription = :subscription
               GROUP BY wmbs_file_details.id, wmbs_file_details.lfn, wmbs_file_details.filesize,
                        wmbs_file_details.events"""
