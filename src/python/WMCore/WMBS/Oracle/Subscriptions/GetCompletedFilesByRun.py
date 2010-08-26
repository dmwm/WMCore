#!/usr/bin/env python
"""
_GetCompletedFilesByRun_

Oracle implementation of Subscription.GetCompletedFilesByRun
"""

__all__ = []
__revision__ = "$Id: GetCompletedFilesByRun.py,v 1.1 2009/05/01 19:42:27 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetCompletedFilesByRun import \
     GetCompletedFilesByRun as GetCompletedFilesByRunMySQL

class GetCompletedFilesByRun(GetCompletedFilesByRunMySQL):
    sql = """SELECT wc.fileid FROM wmbs_sub_files_complete wc
               INNER JOIN wmbs_file_runlumi_map wm ON (wm.fileid = wc.fileid)
             WHERE wc.subscription = :subscription AND run = :run
             """
