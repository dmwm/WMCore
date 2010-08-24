#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.2 2008/11/20 21:52:32 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """insert into wmbs_sub_files_failed 
                (subscription, file) values (:subscription, :file)"""
         
    def execute(self, subscription=None, file=None,
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)