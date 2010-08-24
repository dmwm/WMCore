#!/usr/bin/env python
"""
_ClearFileStatus_
Oracle implementation of Subscriptions.ClearFileStatus

Delete all file status information. For resubmissions and for each state change.
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.ClearFileStatus import ClearFileStatus \
     as ClearFileStatusMySQL

class ClearFileStatus(ClearFileStatusMySQL):
    sql = ["delete from wmbs_sub_files_acquired where subscription=:subscription and fileid=:fileid",
           "delete from wmbs_sub_files_failed where subscription=:subscription and fileid=:fileid",
           "delete from wmbs_sub_files_complete where subscription=:subscription and fileid=:fileid"]
