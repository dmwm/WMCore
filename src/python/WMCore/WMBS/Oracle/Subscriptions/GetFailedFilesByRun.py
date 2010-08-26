#!/usr/bin/env python
"""
_GetFailedFiles_

Oracle implementation of Subscription.GetFailedFiles
"""

__all__ = []
__revision__ = "$Id: GetFailedFilesByRun.py,v 1.1 2009/05/01 19:42:27 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFilesByRun import \
     GetFailedFilesByRun as GetFailedFilesByRunMySQL

class GetFailedFilesByRun(GetFailedFilesByRunMySQL):
    sql = """SELECT wf.fileid FROM wmbs_sub_files_failed wf
                INNER JOIN wmbs_file_runlumi_map wm ON (wm.fileid = wf.fileid)
               WHERE wf.subscription = :subscription AND run = :run
            """