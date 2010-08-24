#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetAcquiredFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.3 2008/11/11 14:01:29 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class GetAcquiredFiles(MySQLBase):
#    sql = """select lfn from wmbs_file_details
#                where id in (select file from wmbs_sub_files_acquired where subscription=:subscription) 
#        """
    sql = "select file from wmbs_sub_files_acquired where subscription=:subscription"
        
    def execute(self, subscription=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription['id']), 
                         conn = conn, transaction = transaction)
        return self.format(result)

