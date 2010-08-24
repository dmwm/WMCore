#!/usr/bin/env python
"""
_DeleteAcquiredFiles_

Oracle implementation of Subscription.DeleteAcquiredFiles

Remove a (list of) file(s) from the aquired state, either due to a state change 
(e.g file has become completed/failed) or as a clean up/resubmission.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.DeleteAcquiredFiles import \
     DeleteAcquiredFiles as DeleteAcquiredFilesMySQL

class DeleteAcquiredFiles(DeleteAcquiredFilesMySQL):
    sql = """ delete from wmbs_sub_files_acquired 
              where subscription=:subscription and fileid=:fileid """