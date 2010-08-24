#!/usr/bin/env python
"""
_ClearFileStatus_

MySQL implementation of Subscriptions.ClearFileStatus

Remove a (list of) file(s) from wmbs_sub_files_[] table, either due to a state change 
(e.g file has become completed/failed) or as a clean up/resubmission.
"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class ClearFileStatus(DBFormatter):
    sql = ["delete from wmbs_sub_files_acquired where subscription=:subscription and file=:fileid",
           "delete from wmbs_sub_files_failed where subscription=:subscription and file=:fileid",
           "delete from wmbs_sub_files_complete where subscription=:subscription and file=:fileid"]
    
    def format(self, result):
        return True
        
    def execute(self, subscription=None, file=None, conn = None, transaction = False):
        binds = []
        if type(file) != list:
            file = [file]
        i = 0    
        for oneFile in file:
            oneBind = self.getBinds(subscription=subscription, fileid=oneFile) * 3
            binds.extend(oneBind)
            i += 1
    
        self.sql =  self.sql * i
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)