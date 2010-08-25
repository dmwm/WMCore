#!/usr/bin/env python
"""
_GetFailedFiles_

Oracle implementation of Subscription.GetFailedFiles
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.GetFailedFilesByRun import \
     GetFailedFilesByRun as GetFailedFilesByRunMySQL

class GetFailedFilesByRun(GetFailedFilesByRunMySQL):
    sql = """SELECT wf.fileid FROM wmbs_sub_files_failed wf
                INNER JOIN wmbs_file_runlumi_map wm ON (wm.fileid = wf.fileid)
               WHERE wf.subscription = :subscription AND run = :run
            """