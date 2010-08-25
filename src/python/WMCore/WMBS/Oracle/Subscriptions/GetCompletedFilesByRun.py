#!/usr/bin/env python
"""
_GetCompletedFilesByRun_

Oracle implementation of Subscription.GetCompletedFilesByRun
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFilesByRun import \
     GetCompletedFilesByRun as GetCompletedFilesByRunMySQL

class GetCompletedFilesByRun(GetCompletedFilesByRunMySQL):
    sql = """SELECT wc.fileid FROM wmbs_sub_files_complete wc
               INNER JOIN wmbs_file_runlumi_map wm ON (wm.fileid = wc.fileid)
             WHERE wc.subscription = :subscription AND run = :run
             """
