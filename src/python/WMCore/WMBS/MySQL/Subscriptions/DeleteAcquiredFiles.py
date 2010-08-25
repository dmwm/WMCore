#!/usr/bin/env python
"""
_DeleteAcquireFiles_

MySQL implementation of Subscription.DeleteAcquiredFiles

Remove a (list of) file(s) from the aquired state, either due to a state change 
(e.g file has become completed/failed) or as a clean up/resubmission.
"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class DeleteAcquiredFiles(DBFormatter):
    sql = "delete from wmbs_sub_files_acquired where subscription=:subscription and file=:fileid"
    
    def format(self, result):
        return True
        
    def execute(self, subscription=None, file=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)