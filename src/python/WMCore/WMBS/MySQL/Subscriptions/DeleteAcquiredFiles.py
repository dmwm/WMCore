#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.DeleteAcquiredFiles

Remove a (list of) file(s) from the aquired state, either due to a state change 
(e.g file has become completed/failed) or as a clean up/resubmission.
"""
__all__ = []
__revision__ = "$Id: DeleteAcquiredFiles.py,v 1.3 2008/11/24 21:46:59 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class DeleteAcquiredFiles(DBFormatter):
    sql = "delete from wmbs_sub_files_acquired where subscription=:subscription and file=:fileid"
    
    def format(self, result):
        return True
        
    def execute(self, subscription=None, file=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)