#!/usr/bin/env python
"""
_CompleteFiles_

MySQL implementation of Subscription.CompleteFiles
"""
__all__ = []
__revision__ = "$Id: CompleteFiles.py,v 1.3 2008/11/24 21:46:59 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class CompleteFiles(DBFormatter):
    sql = """insert into wmbs_sub_files_complete 
                (subscription, file) values (:subscription, :fileid)"""
    
    def format(self, result):
        return True
    
    def execute(self, subscription=None, file=None, 
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)