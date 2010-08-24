#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""
__all__ = []
__revision__ = "$Id: FailFiles.py,v 1.3 2008/11/24 21:46:59 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """insert into wmbs_sub_files_failed 
                (subscription, file) values (:subscription, :fileid)"""
                
    def format(self, result):
        return True
         
    def execute(self, subscription=None, file=None,
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(subscription=subscription, fileid=file), 
                         conn = conn, transaction = transaction)
        return self.format(result)