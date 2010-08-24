#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.2 2008/09/09 16:57:20 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class CompleteFiles(DBFormatter):
    sql = """insert into wmbs_sub_files_complete 
                (subscription, file) values (:subscription, :file)"""
    
    def execute(self, subscription=None, file=None, 
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, file=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)