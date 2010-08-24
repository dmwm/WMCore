#!/usr/bin/env python
"""
_GetFailedFiles_

MySQL implementation of Subscription.GetFailedFiles

Return a list of files that are available for processing
"""
__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.4 2008/11/11 14:01:30 metson Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetFailedFiles(DBFormatter):
#    sql = """select lfn from wmbs_file_details
#                where id in (
#                     select file from wmbs_sub_files_failed where subscription=:subscription
#                 )"""
    sql = "select file from wmbs_sub_files_failed where subscription=:subscription"
    
    def execute(self, subscription=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription['id']), 
                         conn = conn, transaction = transaction)
        return self.format(result)