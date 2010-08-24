#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.DeleteAcquiredFiles

Remove a (list of) file(s) from the aquired state, either due to a state change 
(e.g file has become completed/failed) or as a clean up/resubmission.
"""
__all__ = []
__revision__ = "$Id: DeleteAcquiredFiles.py,v 1.1 2008/06/30 18:01:23 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class DeleteAcquiredFiles(MySQLBase):
    sql = "delete from wmbs_sub_files_acquired where subscription=:subscription and file=:file"
        
    def execute(self, subscription=None, file=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)