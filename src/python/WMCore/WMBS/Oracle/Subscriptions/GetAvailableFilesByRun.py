#!/usr/bin/env python
"""
_GetAvailableFilesByRun_

Oracle implementation of Subscription.GetAvailableFilesByRun
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByRun import \
     GetAvailableFilesByRun as GetAvailableFilesByRunMySQL

class GetAvailableFilesByRun(GetAvailableFilesByRunMySQL):
    sql = """SELECT wmbs_sub_files_available.fileid FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_sub_files_available.fileid = wmbs_file_runlumi_map.fileid
             WHERE wmbs_sub_files_available.subscription = :subscription AND
                   wmbs_file_runlumi_map.run = :run"""
