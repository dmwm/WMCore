#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetAcquiredFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.4 2008/11/20 21:52:32 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetAcquiredFiles(DBFormatter):
#    sql = """select lfn from wmbs_file_details
#                where id in (select file from wmbs_sub_files_acquired where subscription=:subscription) 
#        """
    sql = "select file from wmbs_sub_files_acquired where subscription=:subscription"
        
    def execute(self, subscription=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription['id']), 
                         conn = conn, transaction = transaction)
        return self.format(result)

